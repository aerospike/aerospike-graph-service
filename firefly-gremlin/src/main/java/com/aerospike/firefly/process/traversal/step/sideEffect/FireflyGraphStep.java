package com.aerospike.firefly.process.traversal.step.sideEffect;

import com.aerospike.client.query.IndexType;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyCardinalityMetadata;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Pieter Martin
 * @author Grant Haywood <a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyGraphStep.class);
    private final List<HasContainer> hasContainers = new ArrayList<>();
    private final List<Iterator> iterators = new ArrayList<>();

    public FireflyGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);
        this.setIteratorSupplier(() ->  (Vertex.class.isAssignableFrom(this.returnClass) ? (Iterator<E>)this.vertices() : (Iterator<E>)this.edges()));
    }

    /**
     * Get an iterator of all the edges, with filter if filter is applied
     * if index is available for the filter, use the index to fill the iterator
     *
     * @return iterator of edges
     */
    private Iterator<? extends Edge> edges() {
        // Get FireflyGraph from the traversal.
        final FireflyGraph graph = (FireflyGraph) this.getTraversal().getGraph().get();

        // Grab all Edges for iterator.
        final Iterator<? extends Edge> iterator = elements(
                graph,
                graph.getBaseGraph().EDGE_AERO_SET,
                graph.getBaseGraph().PROPERTIES,
                FireflyEdge.class,
                new FireflyGraph.GetElements<Edge>() {
                    @Override
                    public Iterator<Edge> getFiltered(final List<HasContainer> hasContainers, final Object... ids) {
                        return graph.edges(hasContainers, ids);
                    }

                    @Override
                    public Iterator<Edge> getUnfiltered(final Object... ids) {
                        return graph.edges(ids);
                    }
                },
                graph::edgeFromRecord,
                graph::edgeFromRecord);

        // Return base iterator.
        return iterator;
    }

    /**
     * Get an iterator of all the vertices, with filter if filter is applied
     * if index is available for the filter, use the index to fill the iterator
     *
     * @return iterator of vertices
     */
    private Iterator<? extends Vertex> vertices() {
        // Get FireflyGraph from the traversal.
        final FireflyGraph graph = (FireflyGraph) this.getTraversal().getGraph().get();

        // Grab all Vertices for iterator.
        final Iterator<? extends Vertex> iterator = elements(
                graph,
                graph.getBaseGraph().VERTEX_AERO_SET,
                graph.getBaseGraph().VERTEX_PROPERTY_NAME_TO_VALUE,
                FireflyVertex.class,
                new FireflyGraph.GetElements<Vertex>() {
                    @Override
                    public Iterator<Vertex> getFiltered(final List<HasContainer> hasContainers, final Object... ids) {
                        return graph.vertices(hasContainers, ids);
                    }

                    @Override
                    public Iterator<Vertex> getUnfiltered(final Object... ids) {
                        return graph.vertices(ids);
                    }
                },
                graph::vertexFromRecord,
                graph::vertexFromRecord);

        // Return base iterator.
        return iterator;
    }

    private List<HasContainer> getFireflyHasContainers(final List<HasContainerWithCardinality> hasContainerWithCardinalities) {
        return hasContainerWithCardinalities.stream().filter(c -> !c.isSupported).map(c -> c.hasContainer).collect(Collectors.toList());
    }

    private List<HasContainer> getAerospikeHasContainers(final List<HasContainerWithCardinality> hasContainerWithCardinalities) {
        return hasContainerWithCardinalities.stream().filter(c -> c.isSupported).map(c -> c.hasContainer).collect(Collectors.toList());
    }

    private <R extends Element> Iterator<R> elements(final FireflyGraph graph,
                                                     final String setName,
                                                     final String binName,
                                                     final Class<? extends FireflyElement> elementClass,
                                                     final FireflyGraph.GetElements<R> getElements,
                                                     final FireflyGraph.TransformKeyRecord<R> transformKeyRecord,
                                                     final FireflyGraph.TransformMapEntryKeyRecord<R> transformMapEntryKeyRecord) {
        Iterator<R> iterator;
        final List<HasContainerWithCardinality> sortedHasContainers = getHasContainersWithCardinalityOrder();
        final List<HasContainer> aerospikeSideHasContainers = getAerospikeHasContainers(sortedHasContainers);
        final List<HasContainer> fireflySideHasContainers = getFireflyHasContainers(sortedHasContainers);

        if (null == this.ids) {
            iterator = Collections.emptyIterator();
            iterators.add(iterator);
            return iterator;
        } else if (this.ids.length > 0) {
            iterator = aerospikeSideHasContainers.isEmpty() ?
                    getElements.getUnfiltered(this.ids) :
                    getElements.getFiltered(aerospikeSideHasContainers, this.ids);
            iterator = this.hasContainerCheckedIterator(iterator, fireflySideHasContainers);
            iterators.add(iterator);
            return iterator;
        }

        // If there are HasContainers that can be pushed to aerospike, grab first item (this is highest cardinality based on sorting).
        final HasContainer topContainer = aerospikeSideHasContainers.isEmpty() ? null : aerospikeSideHasContainers.get(0);

        if (topContainer == null || topContainer.getKey() == null ||
                (topContainer.getKey().startsWith("~") && !topContainer.getKey().equals("~label"))) {
            // If index container is null or key is null or if key starts with ~ but is not ~label, then get graph.vertices().
            iterator = getElements.getUnfiltered();
            iterator = this.hasContainerCheckedIterator(iterator, hasContainers);
            iterators.add(iterator);
            return iterator;
        } else if (topContainer.getKey().equals("~label") ||
                Number.class.isAssignableFrom(topContainer.getValue().getClass()) ||
                String.class.isAssignableFrom(topContainer.getValue().getClass())) {
            if (aerospikeSideHasContainers.size() > 0) {
                // Don't want to filter on something we run as our primary discriminator.
                aerospikeSideHasContainers.remove(0);
            }

            // Find index.
            final Optional<FireflyIndexMetadata.IndexInfo> propertyIndexInfo =
                    graph.fireflyIndexMetadata.getPropertyIndexInfo(elementClass, topContainer.getKey(), topContainer.getValue());

            // If we have index, query it, otherwise we need to scan (or error out).
            if (propertyIndexInfo.isPresent()) {
                iterator = graph.queryIndex(propertyIndexInfo.get(),
                        topContainer.getPredicate(),
                        transformKeyRecord,
                        aerospikeSideHasContainers,
                        elementClass);
            } else {
                LOG.debug("No index found for key {} and value {}, running scan", topContainer.getKey(), topContainer.getValue());
                iterator = graph.queryScan(
                        topContainer.getKey(),
                        setName,
                        topContainer.getKey().equals("~label") ? AerospikeConnection.LABEL : binName,
                        topContainer.getPredicate(),
                        transformMapEntryKeyRecord,
                        aerospikeSideHasContainers,
                        elementClass);

            }
            // Need to wrap iterator in hasContainerCheckedIterator() to apply hasContainers that could not be pushed down to Aerospike.
            iterator = this.hasContainerCheckedIterator(iterator, fireflySideHasContainers);
            iterators.add(iterator);
            return iterator;
        } else {
            iterator = Collections.emptyIterator();
            iterators.add(iterator);
            return iterator;
        }
    }

    /**
     * Simple class to contain info about has containers.
     * Public only for testing purposes.
     */
    public static class HasContainerWithCardinality {
        public final HasContainer hasContainer;
        public final boolean isLabel;
        public final boolean isSupported;
        public final FireflyCardinalityMetadata.CardinalityInfo cardinality;

        /**
         * Constructor.
         *
         * @param hasContainer The HasContainer.
         * @param cardinality  The cardinality info.
         */
        public HasContainerWithCardinality(final HasContainer hasContainer, final FireflyCardinalityMetadata.CardinalityInfo cardinality) {
            this.hasContainer = hasContainer;
            this.cardinality = cardinality;
            this.isLabel = "~label".equals(hasContainer.getKey());
            this.isSupported = true;
        }

        /**
         * Constructor.
         *
         * @param hasContainer The HasContainer.
         * @param isSupported  true if the hasContainer has a supported predicate.
         */
        public HasContainerWithCardinality(final HasContainer hasContainer, final boolean isSupported) {
            this.hasContainer = hasContainer;
            this.cardinality = null;
            this.isLabel = "~label".equals(hasContainer.getKey());
            this.isSupported = isSupported;
        }

        /**
         * Constructor.
         *
         * @param hasContainer The HasContainer.
         */
        public HasContainerWithCardinality(final HasContainer hasContainer) {
            this.hasContainer = hasContainer;
            this.cardinality = null;
            this.isLabel = "~label".equals(hasContainer.getKey());
            this.isSupported = true;
        }
    }

    /**
     * Get list of has containers with cardinality info attached.
     * Only public to allow easier testing.
     *
     * @return List of has containers with cardinality info.
     */
    public List<HasContainerWithCardinality> getHasContainersWithCardinalityOrder() {
        final ArrayList<BiPredicate> supportedNumericPredicates = new ArrayList<>() {{
            add(Compare.eq);
            add(Compare.lt);
            add(Compare.gt);
        }};
        final ArrayList<BiPredicate> supportedStringPredicates = new ArrayList<>() {{
            add(Compare.eq);
        }};

        final List<HasContainerWithCardinality> hasContainersWithCardinality = new ArrayList<>();
        final FireflyGraph graph = (FireflyGraph) this.getTraversal().getGraph().get();
        hasContainers.iterator().forEachRemaining(hasContainer -> {
            // TODO GRAPH-368: We should go through expressions and see what kind of
            //  predicates we can push down to Aerospike via Exp.
            if (hasContainer != null && hasContainer.getKey() != null && hasContainer.getValue() == null) {
                hasContainersWithCardinality.add(new HasContainerWithCardinality(hasContainer, false));
            } else if (hasContainer == null || hasContainer.getKey() == null || hasContainer.getValue() == null) {
                hasContainersWithCardinality.add(new HasContainerWithCardinality(hasContainer, false));
            } else if (!Long.class.isAssignableFrom(hasContainer.getValue().getClass()) &&
                    !Integer.class.isAssignableFrom(hasContainer.getValue().getClass()) &&
                    !String.class.isAssignableFrom(hasContainer.getValue().getClass())) {
                // If the HasContainer predicate is for an unsupported type.
                hasContainersWithCardinality.add(new HasContainerWithCardinality(hasContainer, false));
            } else if ((Long.class.isAssignableFrom(hasContainer.getValue().getClass()) ||
                    Integer.class.isAssignableFrom(hasContainer.getValue().getClass()))
                    && !supportedNumericPredicates.contains(hasContainer.getBiPredicate())) {
                // Else if the HasContainer predicate is for a numeric type but is not supported.
                hasContainersWithCardinality.add(new HasContainerWithCardinality(hasContainer, false));
            } else if (String.class.isAssignableFrom(hasContainer.getValue().getClass()) &&
                    !supportedStringPredicates.contains(hasContainer.getBiPredicate())) {
                // Else if the HasContainer predicate is for a string type but is not supported.
                hasContainersWithCardinality.add(new HasContainerWithCardinality(hasContainer, false));
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
                    hasContainersWithCardinality.add(new HasContainerWithCardinality(hasContainer, cardinality));
                } else {
                    hasContainersWithCardinality.add(new HasContainerWithCardinality(hasContainer));
                }
            }
        });


        orderHasContainersWithCardinality(hasContainersWithCardinality);
        return hasContainersWithCardinality;
    }

    private void orderHasContainersWithCardinality(final List<HasContainerWithCardinality> hasContainerWithCardinalities) {
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

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return (null == this.ids || 0 == this.ids.length) ?
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers) :
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids), this.hasContainers);
    }

    static class HasContainerIterator<E extends Element> implements Iterator<E>, AutoCloseable {

        private final Iterator<E> i;
        private final List<HasContainer> hasContainers;
        private E e;
        private boolean valid;

        public HasContainerIterator(final Iterator<E> iterator, final List<HasContainer> hasContainers) {
            this.i = iterator;
            this.hasContainers = hasContainers;
            this.e = null;
            this.valid = false;
        }

        @Override
        public boolean hasNext() {
            // Element found and is waiting to be grabbed.
            if (valid) {
                return true;
            }

            // Find next element that matches HasContainer.
            while (i.hasNext()) {
                e = i.next();
                if (HasContainer.testAll(e, this.hasContainers)) {
                    valid = true;
                    return true;
                }
            }
            valid = false;
            return false;
        }

        @Override
        public E next() {
            // If they checked hasNext() prior to the next() call and there was an element available, valid will be true.
            if (valid) {
                valid = false;
                return e;
            }

            if (hasNext()) {
                valid = false;
                return e;
            }

            // No more elements available.
            throw FastNoSuchElementException.instance();
        }

        @Override
        public void close() {
            CloseableIterator.closeIterator(i);
        }
    }

    private <E extends Element> Iterator<E> hasContainerCheckedIterator(final Iterator<E> iterator, final List<HasContainer> minimalHasContainers) {
        return new HasContainerIterator<>(iterator, minimalHasContainers);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(HasContainer hasContainer) {
        if (hasContainer.getPredicate() instanceof AndP) {
            for (final P<?> predicate : ((AndP<?>) hasContainer.getPredicate()).getPredicates()) {
                this.addHasContainer(new HasContainer(hasContainer.getKey(), predicate));
            }
        } else
            this.hasContainers.add(hasContainer);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }

    @Override
    public void close() {
        iterators.forEach(CloseableIterator::closeIterator);
    }
}
