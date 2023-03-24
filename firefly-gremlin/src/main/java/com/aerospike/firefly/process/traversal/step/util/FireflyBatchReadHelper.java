package com.aerospike.firefly.process.traversal.step.util;

import com.aerospike.client.query.IndexType;
import com.aerospike.firefly.io.FireflyCardinalityMetadata;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyBatchReadHelper {

    public static class ReadStepInfo<E extends Element> {
        final Traverser.Admin<E> traverser;
        final Integer size;

        public ReadStepInfo(final Traverser.Admin<E> traversers, final Integer size) {
            this.traverser = traversers;
            this.size = size;
        }
    }

    /**
     * Template to fill out to allow on the fly KeyRecord to Element mapping.
     *
     * @param <E> Type of element to return.
     */
    public interface ReadElements<E extends Element> {
        List<E> read(final List<HasContainer> hasContainers, final List<FireflyId> unorderedIds);
    }


    public static <E extends FireflyElement, T extends Element> void drainDataToOutput(final Step<T, T> notThat,
                                                                                       final List<FireflyId> fireflyIdList,
                                                                                       final Set<FireflyId> uniqueIdSet,
                                                                                       final Map<FireflyId, E> elementMap,
                                                                                       final List<ReadStepInfo<T>> readInfo,
                                                                                       final List<HasContainer> aerospikeHasContainers,
                                                                                       final List<HasContainer> fireflyHasContainers,
                                                                                       final TraverserSet<T> output,
                                                                                       final ReadElements<E> readElements) {
        // Read all IDs in a batch.
        final List<FireflyId> unorderedIds = new ArrayList<>(uniqueIdSet);
        final List<E> unorderedElements = readElements.read(aerospikeHasContainers, unorderedIds);

        // If there is a mismatch we might have had concurrent removals or expression filtering. To fix this rematch the lists.
        if (unorderedIds.size() != unorderedElements.size()) {
            final Set<FireflyId> unorderedEdgesIds = unorderedElements.stream().map(e -> e.id).collect(Collectors.toSet());
            final Set<FireflyId> missingIds = new HashSet<>(unorderedIds);
            missingIds.removeAll(unorderedEdgesIds);
            unorderedIds.removeAll(missingIds);
        }

        for (int i = 0; i < unorderedIds.size(); i++) {
            elementMap.put(unorderedIds.get(i), unorderedElements.get(i));
        }

        // Loop through the info list and assign the appropriate number of vertices to each traverser using the info.
        int i = 0;
        for (final ReadStepInfo<T> info : readInfo) {
            for (int j = 0; j < info.size; j++) {
                // Create a new traverser with the edge and add it to the output set using the split.
                // Note, this is invoked info.size times.
                final T element = (T) elementMap.get(fireflyIdList.get(i++));

                // Check firefly has containers to ensure we apply all predicates.
                if (element == null || !HasContainer.testAll(element, fireflyHasContainers)) {
                    // Element was not found - this is because it was deleted concurrently or filtered via expression.
                    continue;
                }
                output.add(info.traverser.split(element, notThat));
            }
        }

        // Clear intermediate buffers.
        fireflyIdList.clear();
        uniqueIdSet.clear();
        readInfo.clear();
    }

    public static <E extends FireflyElement> void addElementsToSet(final List<FireflyId> fireflyIdList,
                                                                   final Set<FireflyId> uniqueIdSet,
                                                                   final Map<FireflyId, E> fireflyElementMap,
                                                                   final List<FireflyId> elementIds) {
        fireflyIdList.addAll(elementIds);
        for (final FireflyId id : elementIds) {
            if (!fireflyElementMap.containsKey(id)) {
                uniqueIdSet.add(id);
            }
        }
    }

    /**
     * Get list of has containers with cardinality info attached.
     * Only public to allow easier testing.
     *
     * @return List of has containers with cardinality info.
     */
    public static <E extends Element> List<FireflyGraphStep.HasContainerWithCardinality> getHasContainersWithCardinalityOrder(final FireflyGraph graph,
                                                                                                                              final Class<E> returnClass,
                                                                                                                              final List<HasContainer> hasContainers) {
        final ArrayList<BiPredicate> supportedNumericPredicates = new ArrayList<>() {{
            add(Compare.eq);
            add(Compare.lt);
            add(Compare.gt);
            // add(Compare.neq); Can't support this since sindexes do not.
            add(Compare.lte);
            add(Compare.gte);
        }};
        final ArrayList<BiPredicate> supportedStringPredicates = new ArrayList<>() {{
            add(Compare.eq);
        }};

        final List<FireflyGraphStep.HasContainerWithCardinality> hasContainersWithCardinality = new ArrayList<>();
        hasContainers.iterator().forEachRemaining(hasContainer -> {
            // TODO GRAPH-368: We should go through expressions and see what kind of
            //  predicates we can push down to Aerospike via Exp.
            if (hasContainer != null && hasContainer.getKey() != null && hasContainer.getValue() == null) {
                hasContainersWithCardinality.add(new FireflyGraphStep.HasContainerWithCardinality(hasContainer, false));
            } else if (hasContainer == null || hasContainer.getKey() == null || hasContainer.getValue() == null) {
                hasContainersWithCardinality.add(new FireflyGraphStep.HasContainerWithCardinality(hasContainer, false));
            } else if (!Long.class.isAssignableFrom(hasContainer.getValue().getClass()) &&
                    !Integer.class.isAssignableFrom(hasContainer.getValue().getClass()) &&
                    !String.class.isAssignableFrom(hasContainer.getValue().getClass())) {
                // If the HasContainer predicate is for an unsupported type.
                hasContainersWithCardinality.add(new FireflyGraphStep.HasContainerWithCardinality(hasContainer, false));
            } else if ((Long.class.isAssignableFrom(hasContainer.getValue().getClass()) ||
                    Integer.class.isAssignableFrom(hasContainer.getValue().getClass()))
                    && !supportedNumericPredicates.contains(hasContainer.getBiPredicate())) {
                // Else if the HasContainer predicate is for a numeric type but is not supported.
                hasContainersWithCardinality.add(new FireflyGraphStep.HasContainerWithCardinality(hasContainer, false));
            } else if (String.class.isAssignableFrom(hasContainer.getValue().getClass()) &&
                    !supportedStringPredicates.contains(hasContainer.getBiPredicate())) {
                // Else if the HasContainer predicate is for a string type but is not supported.
                hasContainersWithCardinality.add(new FireflyGraphStep.HasContainerWithCardinality(hasContainer, false));
            } else {
                // Else the HasContainer predicate is supported.
                final Optional<FireflyIndexMetadata.IndexInfo> indexInfo = graph.fireflyIndexMetadata.getPropertyIndexInfo(
                        Vertex.class.isAssignableFrom(returnClass) ? FireflyVertex.class : FireflyEdge.class,
                        hasContainer.getKey(),
                        hasContainer.getValue());

                if (indexInfo.isPresent()) {
                    final FireflyCardinalityMetadata.CardinalityInfo cardinality;
                    if ("~label".equals(hasContainer.getKey())) {
                        if (Vertex.class.isAssignableFrom(returnClass)) {
                            cardinality = graph.fireflyCardinalityMetadata.getVertexLabelCardinality().orElse(null);
                        } else {
                            cardinality = graph.fireflyCardinalityMetadata.getEdgeLabelCardinality().orElse(null);
                        }
                    } else {
                        final IndexType indexType = String.class.isAssignableFrom(hasContainer.getValue().getClass()) ? IndexType.STRING : IndexType.NUMERIC;
                        if (Vertex.class.isAssignableFrom(returnClass)) {
                            cardinality = graph.fireflyCardinalityMetadata.getVertexPropertyCardinality(hasContainer.getKey(), indexType).orElse(null);
                        } else {
                            cardinality = graph.fireflyCardinalityMetadata.getEdgePropertyCardinality(hasContainer.getKey(), indexType).orElse(null);
                        }
                    }
                    hasContainersWithCardinality.add(new FireflyGraphStep.HasContainerWithCardinality(hasContainer, cardinality));
                } else {
                    hasContainersWithCardinality.add(new FireflyGraphStep.HasContainerWithCardinality(hasContainer));
                }
            }
        });


        orderHasContainersWithCardinality(hasContainersWithCardinality);
        return hasContainersWithCardinality;
    }

    private static void orderHasContainersWithCardinality(final List<FireflyGraphStep.HasContainerWithCardinality> hasContainerWithCardinalities) {
        // -1 -> o1 is better than o2
        // 0 -> o1 is equal to o2
        // 1 -> o1 is worse than o2
        final int O1Best = -1;
        final int O2Best = 1;
        final int O1O2Equal = 0;
        hasContainerWithCardinalities.sort((o1, o2) -> {
            if (o1.cardinality == null && o2.cardinality == null) {
                // Neither are indexed.
                // Properties > labels since properties are likely to be higher cardinality.
                if (o1.isLabel && !o2.isLabel) {
                    return O2Best;
                } else if (!o1.isLabel && o2.isLabel) {
                    return O1Best;
                } else {
                    // Both are either labels or both are properties, but neither are indexed.
                    // We don't know which is better, so we'll just say they're equal.
                    return O1O2Equal;
                }
            } else if (o1.cardinality == null) {
                // o1 is not indexed but o2 is.
                return O2Best;
            } else if (o2.cardinality == null) {
                // o2 is not indexed but o1 is.
                return O1Best;
            } else {
                // This is the case where both are indexed.
                if (o1.isLabel && !o2.isLabel) {
                    // Properties > labels
                    return O2Best;
                } else if (!o1.isLabel && o2.isLabel) {
                    return O1Best;
                } else {
                    // Both are either properties or both are labels.
                    // We'll compare the cardinalities.
                    //
                    // Entries per bval gives you how many entries there are on average
                    // for each unique value.
                    final long o1BVal = o1.cardinality.entriesPerBval;
                    final long o2BVal = o2.cardinality.entriesPerBval;

                    // Long comparison gives us exactly what we want.
                    return Long.compare(o1BVal, o2BVal);
                }
            }
        });
    }

    public static List<HasContainer> getFireflyHasContainers(final List<FireflyGraphStep.HasContainerWithCardinality> hasContainerWithCardinalities) {
        return hasContainerWithCardinalities.stream().filter(c -> !c.isSupported).map(c -> c.hasContainer).collect(Collectors.toList());
    }

    public static List<HasContainer> getAerospikeHasContainers(final List<FireflyGraphStep.HasContainerWithCardinality> hasContainerWithCardinalities) {
        return hasContainerWithCardinalities.stream().filter(c -> c.isSupported).map(c -> c.hasContainer).collect(Collectors.toList());
    }
}
