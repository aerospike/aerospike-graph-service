package com.aerospike.firefly.process.traversal.step.computer;

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.process.traversal.step.util.TraversalUtil;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyCompositeIdStepLocal extends VertexStep<Vertex> implements PrecomputableComputerStep<Vertex> {
    private final Direction direction;
    private final Set<String> edgeLabels;

    // HasContainers to apply to the read of the composite id step to filter results.
    public final List<HasContainer> fireflyHasContainers;
    public final List<HasContainer> aerospikeHasContainers;
    private final List<String> requiredProperties;
    final Traversal.Admin traversal;
    final Set<String> labels;
    private static final ThreadLocal<Map<FireflyId, FireflyVertex>> cache =
            ThreadLocal.withInitial(HashMap::new);
    private static final ThreadLocal<List<Pair<Traverser.Admin<Vertex>, FireflyVertex>>> inputCache =
            ThreadLocal.withInitial(ArrayList::new);

    private static final ThreadLocal<Boolean> firstRun = ThreadLocal.withInitial(() -> true);

    public FireflyCompositeIdStepLocal(final Traversal.Admin traversal,
                                       final Direction direction,
                                       final String[] edgeLabels,
                                       final Set<String> labels,
                                       final List<HasContainer> hasContainers,
                                       final List<String> requiredProperties) {
        super(traversal, Vertex.class, direction, edgeLabels);
        this.traversal = traversal;
        this.direction = direction;
        this.edgeLabels = new HashSet<>(Arrays.asList(edgeLabels));
        this.labels = new HashSet<>(labels);
        if (hasContainers != null) {
            final List<FireflyGraphStep.HasContainerWithCardinality> hasContainerWithCardinalities =
                    FireflyBatchReadHelper.getHasContainersWithCardinalityOrder((FireflyGraph) traversal.getGraph().get(), Vertex.class, hasContainers);
            // TODO GRAPH-401: This is a hack to get around the fact that we cannot filter our cache with a hasContainer.
            //  To get around this we have to filter everything post read again, so all containers pushed to firefly no
            //  matter what.
            fireflyHasContainers = hasContainerWithCardinalities.stream().map(a -> a.hasContainer).collect(Collectors.toList());
            aerospikeHasContainers = FireflyBatchReadHelper.getAerospikeHasContainers(hasContainerWithCardinalities);
        } else {
            fireflyHasContainers = List.of();
            aerospikeHasContainers = List.of();
        }
        this.requiredProperties = requiredProperties;
        for (final String label : labels) {
            this.addLabel(label);
        }

        firstRun.set(true);
    }

    @Override
    public void addStart(final Traverser.Admin<Vertex> start) {
        super.addStart(start);
        System.out.println("FireflyCompositeIdStepLocal.addStart: " + start);
        add(start, start.get());
    }

    public void add(final Traverser.Admin<?> tv, final Vertex v) {
        // System.out.println("FireflyCompositeIdStepLocal.add: " + tv + "; " + v);
        inputCache.get().add(new Pair<>() {
            @Override
            public FireflyVertex setValue(final FireflyVertex value) {
                return null;
            }

            final Traverser.Admin<Vertex> traverser = (Traverser.Admin<Vertex>) tv;
            final FireflyVertex vertex = (FireflyVertex) v;

            @Override
            public Traverser.Admin<Vertex> getLeft() {
                return traverser;
            }

            @Override
            public FireflyVertex getRight() {
                return vertex;
            }
        });
    }

    public void precompute() {
        System.out.println("FireflyCompositeIdStepLocal.precompute");
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());

        // Info is used to keep track of how many output items we assign for each input (executed in order).
        final List<FireflyBatchReadHelper.ReadStepInfo<?>> fireflyCompositeIdStepInfos = new ArrayList<>();
        final List<FireflyId> fireflyIdList = new ArrayList<>();
        final Set<FireflyId> uniqueIdSet = new HashSet<>();
        final Map<FireflyId, FireflyVertex> fireflyVertexMap = new TreeMap<>();

        for (final Pair<Traverser.Admin<Vertex>, FireflyVertex> pair : inputCache.get()) {
            // Get next input traverser and get the FireflyVertex form of it.
            final FireflyVertex vertex = pair.getRight();

            // Latch the size of the current id list.
            final int previousSize = fireflyIdList.size();

            // All the work for supernode scan/index/cache handling is done in the getVertexIdsFromVertex function.
            TraversalUtil.supernodeTraversalWarning(graph, this.traversal, vertex);

            FireflyBatchReadHelper.addElementsToSet(
                    fireflyIdList, uniqueIdSet, fireflyVertexMap, vertex.getVertexIdsFromVertex(direction, edgeLabels));

            // Calculate how many ids were added by the function (size of list - previous size).
            // Create composite id info with this value and the appropriate traverser to the info list.
            fireflyCompositeIdStepInfos.add(new FireflyBatchReadHelper.ReadStepInfo<>(pair.getLeft(), fireflyIdList.size() - previousSize));

            // If we reach or exceed batch size then execute so we don't use too much memory at any given point. Also drain if list size gets very big.
            if (uniqueIdSet.size() >= graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE ||
                    fireflyIdList.size() >= 5 * graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE) {
                // Drain data to output.
                FireflyBatchReadHelper.drainDataToCache(fireflyIdList, uniqueIdSet,
                        fireflyVertexMap, fireflyCompositeIdStepInfos, aerospikeHasContainers, fireflyHasContainers, cache.get(), graph::readVertices, requiredProperties);
            }
        }

        System.out.println("  reading: " + fireflyIdList);
        // Drain data to output.
        FireflyBatchReadHelper.drainDataToCache(fireflyIdList, uniqueIdSet,
                fireflyVertexMap, fireflyCompositeIdStepInfos, aerospikeHasContainers, fireflyHasContainers, cache.get(), graph::readVertices, requiredProperties);
    }

    @Override
    protected Iterator<Vertex> flatMap(final Traverser.Admin<Vertex> traverser) {
        if (firstRun.get()) {
            precompute();
            firstRun.set(false);
        }

        if (cache.get() == null) {
            final Iterator<Vertex> vertices = traverser.get().vertices(this.direction, super.getEdgeLabels());
            return FireflyCloseableIteratorUtils.filter(vertices, v -> HasContainer.testAll(v, fireflyHasContainers));
        } else {
            System.out.println("FireflyCompositeIdStepLocal.flatMap: " + traverser);
            final List<Vertex> output = new ArrayList<>();
            final List<FireflyId> missingIds = new ArrayList<>();
            final FireflyVertex fireflyVertex = traverser.get() instanceof FireflyVertex
                    ? (FireflyVertex) traverser.get()
                    : (FireflyVertex) ((ComputerGraph.ComputerVertex) traverser.get()).getBaseVertex();
            final List<Vertex> finalOutput = output;
            fireflyVertex.getVertexIdsFromVertex(direction, edgeLabels).forEachRemaining(id -> {
                if (cache.get().containsKey(id)) {
                    finalOutput.add(cache.get().get(id));
                } else {
                    missingIds.add(id);
                }
            });
            final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();
            final List<FireflyVertex> vertices = graph.readVertices(aerospikeHasContainers, missingIds, requiredProperties);
            output.addAll(vertices);
            System.out.println("  output: " + output);
            return FireflyCloseableIteratorUtils.filter(output.iterator(), v -> HasContainer.testAll(v, fireflyHasContainers));
        }
    }

    @Override
    public void release() {
        cache.get().clear();
        inputCache.get().clear();
    }
}
