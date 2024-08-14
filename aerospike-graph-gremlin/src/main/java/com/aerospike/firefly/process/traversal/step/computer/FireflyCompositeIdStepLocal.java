package com.aerospike.firefly.process.traversal.step.computer;

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.process.traversal.step.util.TraversalUtil;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.commons.lang3.tuple.Pair;
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
    private static final ThreadLocal<Map<Traverser.Admin<Vertex>, Pair<FireflyVertex, List<Vertex>>>> cache =
            ThreadLocal.withInitial(HashMap::new);

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

    public void add(final Traverser.Admin<?> vertex, final Vertex v) {
        //System.out.println(Thread.currentThread().getName() + " - " + Thread.currentThread().getId() + " - add");
        cache.get().putIfAbsent((Traverser.Admin<Vertex>) vertex, new Pair<>() {
            final FireflyVertex fireflyVertex = (FireflyVertex) v;
            final List<Vertex> vertices = new ArrayList<>();

            @Override
            public FireflyVertex getLeft() {
                return fireflyVertex;
            }

            @Override
            public List<Vertex> getRight() {
                return vertices;
            }

            @Override
            public List<Vertex> setValue(final List<Vertex> value) {
                return null;
            }
        });
    }

    public void precompute() {
        //System.out.println(Thread.currentThread().getName() + " - " + Thread.currentThread().getId() + " - precompute");
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());

        // Info is used to keep track of how many output items we assign for each input (executed in order).
        final List<FireflyBatchReadHelper.ReadStepInfo<Vertex>> fireflyCompositeIdStepInfos = new ArrayList<>();
        final List<FireflyId> fireflyIdList = new ArrayList<>();
        final Set<FireflyId> uniqueIdSet = new HashSet<>();
        final Map<FireflyId, FireflyVertex> fireflyVertexMap = new TreeMap<>();
        int count = 0;

        for (final Traverser.Admin<Vertex> traverser : cache.get().keySet()) {
            count++;
            // Get next input traverser and get the FireflyVertex form of it.
            traverser.setStepId(this.getNextStep().getId());
            final FireflyVertex vertex = cache.get().get(traverser).getLeft();

            // Latch the size of the current id list.
            final int previousSize = fireflyIdList.size();

            // All the work for supernode scan/index/cache handling is done in the getVertexIdsFromVertex function.
            TraversalUtil.supernodeTraversalWarning(graph, this.traversal, vertex);

            FireflyBatchReadHelper.addElementsToSet(
                    fireflyIdList, uniqueIdSet, fireflyVertexMap, vertex.getVertexIdsFromVertex(direction, edgeLabels));

            // Calculate how many ids were added by the function (size of list - previous size).
            // Create composite id info with this value and the appropriate traverser to the info list.
            fireflyCompositeIdStepInfos.add(new FireflyBatchReadHelper.ReadStepInfo<>(traverser, fireflyIdList.size() - previousSize));

            // If we reach or exceed batch size then execute so we don't use too much memory at any given point. Also drain if list size gets very big.
            if (uniqueIdSet.size() >= graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE ||
                    fireflyIdList.size() >= 5 * graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE) {
                // Drain data to output.
                FireflyBatchReadHelper.drainDataToCache(fireflyIdList, uniqueIdSet,
                        fireflyVertexMap, fireflyCompositeIdStepInfos, aerospikeHasContainers, fireflyHasContainers, cache.get(), graph::readVertices, requiredProperties);
            }
        }
        System.out.println("!!!!!!!! input " + this.hashCode() + "-> " + count);

        // Drain data to output.
        FireflyBatchReadHelper.drainDataToCache(fireflyIdList, uniqueIdSet,
                fireflyVertexMap, fireflyCompositeIdStepInfos, aerospikeHasContainers, fireflyHasContainers, cache.get(), graph::readVertices, requiredProperties);
    }

    @Override
    protected Iterator<Vertex> flatMap(final Traverser.Admin<Vertex> traverser) {
        //System.out.println(Thread.currentThread().getName() + " - " + Thread.currentThread().getId() + " - flatmap");
        if (cache.get() == null) {
            System.out.println("No cache");
            return traverser.get().vertices(this.direction, super.getEdgeLabels());
        } else if (cache.get().containsKey(traverser)) {
            System.out.println("Cache hit");
            return cache.get().get(traverser).getRight().iterator();
        } else {
            System.out.println("Cache miss");
            return traverser.get().vertices(this.direction, super.getEdgeLabels());
        }
    }

    @Override
    public void reset() {
        System.out.println(Thread.currentThread().getName() + " - " + Thread.currentThread().getId() + " - reset");
        cache.remove();
        cache.set(new HashMap<>());
        super.reset();
    }

}
