package com.aerospike.firefly.process.traversal.step.computer;

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.process.traversal.step.util.TraversalUtil;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
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
public class FireflyCompositeIdStepLocal extends VertexStep<Vertex> implements PrecomputableComputerStep {
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

    public FireflyCompositeIdStepLocal(final Traversal.Admin traversal,
                                       final Direction direction,
                                       final String[] edgeLabels,
                                       final Set<String> labels,
                                       final List<HasContainer> hasContainers,
                                       final int barrierSize,
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
    }

    public void add(final Traverser.Admin<?> tv, final Vertex v) {
        //System.out.println(Thread.currentThread().getName() + " - " + Thread.currentThread().getId() + " - add");
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
        System.out.println(Thread.currentThread().getName() + " - " + Thread.currentThread().getId() + " - precompute");
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());

        // Info is used to keep track of how many output items we assign for each input (executed in order).
        final List<FireflyBatchReadHelper.ReadStepInfo<Vertex>> fireflyCompositeIdStepInfos = new ArrayList<>();
        final List<FireflyId> fireflyIdList = new ArrayList<>();
        final Set<FireflyId> uniqueIdSet = new HashSet<>();
        final Map<FireflyId, FireflyVertex> fireflyVertexMap = new TreeMap<>();
        int count = 0;

        for (final Pair<Traverser.Admin<Vertex>, FireflyVertex> pair : inputCache.get()) {
            count++;
            // Get next input traverser and get the FireflyVertex form of it.
            pair.getLeft().setStepId(this.getNextStep().getId());
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

        // Drain data to output.
        FireflyBatchReadHelper.drainDataToCache(fireflyIdList, uniqueIdSet,
                fireflyVertexMap, fireflyCompositeIdStepInfos, aerospikeHasContainers, fireflyHasContainers, cache.get(), graph::readVertices, requiredProperties);
    }

    @Override
    protected Iterator<Vertex> flatMap(final Traverser.Admin<Vertex> traverser) {
        System.out.println(Thread.currentThread().getName() + " - " + Thread.currentThread().getId() + " - flatmap");
        if (cache.get() == null) {
            System.out.println("No cache");
            return traverser.get().vertices(this.direction, super.getEdgeLabels());
        } else {
            List<Vertex> output = new ArrayList<>();
            List<FireflyId> missingIds = new ArrayList<>();
            FireflyVertex fireflyVertex = (FireflyVertex) ((ComputerGraph.ComputerVertex) traverser.get()).getBaseVertex();
            fireflyVertex.getVertexIdsFromVertex(direction, edgeLabels).forEachRemaining(id -> {
                if (cache.get().containsKey(id)) {
                    System.out.println("Cache hit");
                    output.add(cache.get().get(id));
                } else {
                    System.out.println("Cache miss");
                    missingIds.add(id);
                }
            });
            final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();
            System.out.println("Missing ids: " + missingIds.size());
            List<FireflyVertex> vertices = graph.readVertices(List.of(), missingIds, requiredProperties);
            System.out.println("Vertices: " + vertices.size());
            output.addAll(vertices);
            System.out.println("output: " + output.size());
            return output.iterator();
        }
    }

    @Override
    public void release() {
        cache.get().clear();
        inputCache.get().clear();
    }
}
