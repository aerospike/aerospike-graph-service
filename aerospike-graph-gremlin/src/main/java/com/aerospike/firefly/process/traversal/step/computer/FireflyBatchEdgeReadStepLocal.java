package com.aerospike.firefly.process.traversal.step.computer;

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.process.traversal.step.util.TraversalUtil;
import com.aerospike.firefly.structure.FireflyEdge;
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
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyBatchEdgeReadStepLocal extends VertexStep<Edge> implements PrecomputableComputerStep<Edge> {
    private final Direction direction;
    private final Set<String> edgeLabels;

    // HasContainers to apply to the read of the composite id step to filter results.
    public final List<HasContainer> fireflyHasContainers;
    public final List<HasContainer> aerospikeHasContainers;
    private final List<String> requiredProperties;
    final Traversal.Admin traversal;
    final Set<String> labels;
    private static final ThreadLocal<Map<FireflyId, FireflyEdge>> cache =
            ThreadLocal.withInitial(HashMap::new);
    private static final ThreadLocal<List<Pair<Traverser.Admin<Vertex>, FireflyVertex>>> inputCache =
            ThreadLocal.withInitial(ArrayList::new);

    public FireflyBatchEdgeReadStepLocal(final Traversal.Admin traversal,
                                         final Direction direction,
                                         final String[] edgeLabels,
                                         final Set<String> labels,
                                         final List<HasContainer> hasContainers,
                                         final List<String> requiredProperties) {
        super(traversal, Edge.class, direction, edgeLabels);
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
    }

    public void add(final Traverser.Admin<?> tv, final Vertex v) {
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
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());

        // Info is used to keep track of how many output items we assign for each input (executed in order).
        final List<FireflyBatchReadHelper.ReadStepInfo<?>> fireflyBatchEdgeReadStepInfos = new ArrayList<>();
        final List<FireflyId> fireflyIdList = new ArrayList<>();
        final Set<FireflyId> uniqueIdSet = new HashSet<>();
        final Map<FireflyId, FireflyEdge> fireflyEdgeMap = new HashMap<>();

        for (final Pair<Traverser.Admin<Vertex>, FireflyVertex> pair : inputCache.get()) {
            // Get next input traverser and get the FireflyVertex form of it.
            final FireflyVertex vertex = pair.getRight();

            // Latch the size of the current id list.
            final int previousSize = fireflyIdList.size();

            // All the work for supernode scan/index/cache handling is done in the getVertexIdsFromVertex function.
            TraversalUtil.supernodeTraversalWarning(graph, this.traversal, vertex);

            vertex.getBatchedEdgeIdsFromVertex(direction, edgeLabels, fireflyIdList, aerospikeHasContainers);
            for (int i = previousSize; i < fireflyIdList.size(); i++) {
                final FireflyId id = fireflyIdList.get(i);
                if (!fireflyEdgeMap.containsKey(id)) {
                    uniqueIdSet.add(id);
                }
            }

            // Calculate how many ids were added by the function (size of list - previous size).
            // Create composite id info with this value and the appropriate traverser to the info list.
            fireflyBatchEdgeReadStepInfos.add(new FireflyBatchReadHelper.ReadStepInfo<>(pair.getLeft(), fireflyIdList.size() - previousSize));

            // If we reach or exceed batch size then execute so we don't use too much memory at any given point. Also drain if list size gets very big.
            if (uniqueIdSet.size() >= graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE ||
                    fireflyIdList.size() >= 5 * graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE) {
                // Drain data to output. No need to pass in aerospikeHasContainers since they were used to filter Edge IDs already.
                FireflyBatchReadHelper.drainDataToCache(fireflyIdList, uniqueIdSet,
                        fireflyEdgeMap, fireflyBatchEdgeReadStepInfos, Collections.emptyList(), fireflyHasContainers, cache.get(), graph::readEdges, null);
            }
        }

        FireflyBatchReadHelper.drainDataToCache(fireflyIdList, uniqueIdSet,
                fireflyEdgeMap, fireflyBatchEdgeReadStepInfos, Collections.emptyList(), fireflyHasContainers, cache.get(), graph::readEdges, null);
    }

    @Override
    protected Iterator<Edge> flatMap(final Traverser.Admin<Vertex> traverser) {
        if (cache.get() == null) {
            final Iterator<Edge> edges = traverser.get().edges(this.direction, super.getEdgeLabels());
            return FireflyCloseableIteratorUtils.filter(edges, e -> HasContainer.testAll(e, fireflyHasContainers));
        } else {
            final List<Edge> output = new ArrayList<>();
            final List<FireflyId> missingIds = new ArrayList<>();
            FireflyVertex fireflyVertex = (FireflyVertex) ((ComputerGraph.ComputerVertex) traverser.get()).getBaseVertex();
            fireflyVertex.getEdgeIdsFromVertex(direction, edgeLabels, aerospikeHasContainers).forEachRemaining(id -> {
                if (cache.get().containsKey(id)) {
                    output.add(cache.get().get(id));
                } else {
                    missingIds.add(id);
                }
            });
            final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();
            final List<FireflyEdge> edges = graph.readEdges(List.of(), missingIds, requiredProperties);
            output.addAll(edges);
            return FireflyCloseableIteratorUtils.filter(output.iterator(), e -> HasContainer.testAll(e, fireflyHasContainers));
        }
    }

    @Override
    public void release() {
        cache.get().clear();
        inputCache.get().clear();
    }
}
