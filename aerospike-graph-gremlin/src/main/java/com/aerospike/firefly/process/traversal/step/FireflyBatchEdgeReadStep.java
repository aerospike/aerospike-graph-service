package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.process.traversal.step.util.TraversalUtil;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static com.aerospike.firefly.util.config.ConfigurationHelper.getTraversalOptionInteger;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyBatchEdgeReadStep extends CollectingBarrierStep<Edge> implements LocalBarrier<Edge> {
    private final Direction direction;
    private final Set<String> edgeLabels;
    public final List<HasContainer> fireflyHasContainers;
    public final List<HasContainer> aerospikeHasContainers;
    private final int barrierSize;
    private final ExecutorService executorService;


    public FireflyBatchEdgeReadStep(final Traversal.Admin traversal,
                                    final Direction direction,
                                    final String[] edgeLabels,
                                    final Set<String> labels,
                                    final List<HasContainer> hasContainers,
                                    final int barrierSize) {
        super(traversal, barrierSize);
        this.direction = direction;
        this.edgeLabels = new HashSet<>(Arrays.asList(edgeLabels));
        this.labels = new HashSet<>(labels);
        this.barrierSize = barrierSize;
        if (hasContainers != null) {
            final List<FireflyGraphStep.HasContainerWithCardinality> hasContainerWithCardinalities =
                    FireflyBatchReadHelper.getHasContainersWithCardinalityOrder((FireflyGraph) getTraversal().getGraph().get(), Edge.class, hasContainers);
            // TODO GRAPH-401: This is a hack to get around the fact that we cannot filter our cache with a hasContainer.
            //  To get around this we have to filter everything post read again, so all containers pushed to firefly no
            //  matter what.
            fireflyHasContainers = hasContainerWithCardinalities.stream().map(a -> a.hasContainer).collect(Collectors.toList());
            aerospikeHasContainers = FireflyBatchReadHelper.getAerospikeHasContainers(hasContainerWithCardinalities);
        } else {
            fireflyHasContainers = List.of();
            aerospikeHasContainers = List.of();
        }
        final Optional<Integer> threads = getTraversalOptionInteger(ConfigurationHelper.TraversalOptions.PARALLELIZE, traversal, 1, Integer.MAX_VALUE);
        executorService = threads.map(integer -> Executors.newFixedThreadPool(integer, r -> {
            final Thread t = new Thread(r);
            t.setName("Aerospike-Graph-BatchEdgeRead-Worker-" + t.getId());
            t.setDaemon(true);
            return t;
        })).orElse(null);
    }

    private void parallelBarrierConsumer(final TraverserSet<Edge> set) {
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());
        FireflyBatchReadHelper.pullFromLeft(traversal, graph, set, barrierSize);

        // Info is used to keep track of how many output items we assign for each input (executed in order).
        final Map<FireflyId, FireflyEdge> fireflyEdgeMap = new ConcurrentHashMap<>();
        final Map<Element, List<FireflyId>> duplicateIdMap = new HashMap<>();
        final Map<Element, Future<List<FireflyId>>> futures = new HashMap<>();
        final List<Pair<Element, Traverser.Admin<?>>> orderedElements = new ArrayList<>();

        // Run through initial set, kick off indexes in background on supernodes and grab ids for batch reading.
        while (!set.isEmpty()) {
            // Get next input traverser and get the FireflyVertex form of it.
            final Traverser.Admin<Edge> traverser = set.remove();
            final FireflyVertex vertex = (FireflyVertex) traverser.get();
            orderedElements.add(new Pair<>(vertex, traverser));

            // Latch the size of the current id list.
            if (vertex.isEdgeCacheOverflowed()) {
                if (!futures.containsKey(vertex)) {
                    futures.put(vertex, executorService.submit(() -> {
                        final List<FireflyId> ids = new ArrayList<>();
                        vertex.getBatchedEdgeIdsFromVertex(direction, edgeLabels, ids, aerospikeHasContainers, fireflyEdgeMap);
                        return ids;
                    }));
                }
            } else {
                if (!duplicateIdMap.containsKey(vertex) || duplicateIdMap.get(vertex) == null) {
                    final List<FireflyId> ids = new ArrayList<>();
                    vertex.getBatchedEdgeIdsFromVertex(direction, edgeLabels, ids, aerospikeHasContainers);
                    duplicateIdMap.put(vertex, ids);
                }
            }
        }

        // Run batch reads in background while indexes are running.
        final Set<FireflyId> uniqueIds = new HashSet<>();
        final List<Future<?>> batchReadFutures = new ArrayList<>();
        for (final List<FireflyId> entry : duplicateIdMap.values()) {
            for (final FireflyId id : entry) {
                if (!fireflyEdgeMap.containsKey(id))
                    uniqueIds.add(id);
            }
            if (uniqueIds.size() > graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE) {
                final List<FireflyId> idsToRead = new ArrayList<>(uniqueIds);
                batchReadFutures.add(executorService.submit(() -> {
                            final List<FireflyEdge> edges = graph.readEdges(aerospikeHasContainers, idsToRead, null);
                            for (final FireflyEdge edge : edges) {
                                fireflyEdgeMap.put(edge.id, edge);
                            }
                        }
                ));
                uniqueIds.clear();
            }
        }
        if (!uniqueIds.isEmpty()) {
            final List<FireflyId> idsToRead = new ArrayList<>(uniqueIds);
            batchReadFutures.add(executorService.submit(() -> {
                        final List<FireflyEdge> edges = graph.readEdges(aerospikeHasContainers, idsToRead, null);
                        for (final FireflyEdge edge : edges) {
                            fireflyEdgeMap.put(edge.id, edge);
                        }
                    }
            ));
        }

        // Wait for batch reads to finish before processing output.
        try {
            for (final Future<?> future : batchReadFutures) {
                future.get();
            }
        } catch (final InterruptedException e) {
            throw new TraversalInterruptedException();
        } catch (final ExecutionException e) {
            sneakyThrow(e);
        }

        // All the data is now in the fireflyEdgeMap, process the output.
        try {
            for (final Pair<Element, Traverser.Admin<?>> pair : orderedElements) {
                final Element element = pair.getValue0();
                final Traverser.Admin traverser = pair.getValue1();
                final List<FireflyId> ids = futures.containsKey(element) ? futures.get(element).get() : duplicateIdMap.get(element);
                final List<FireflyEdge> edges = ids.stream().map(fireflyEdgeMap::get).collect(Collectors.toList());
                for (final FireflyEdge edge : edges) {
                    if (edge != null && HasContainer.testAll(edge, fireflyHasContainers)) {
                        set.add(traverser.split(edge, this));
                    }
                }
            }
        } catch (final InterruptedException e) {
            throw new TraversalInterruptedException();
        } catch (final ExecutionException e) {
            sneakyThrow(e);
        }

        if (set.isEmpty()) {
            set.add(EmptyTraverser.instance());
        }
    }

    @Override
    public void barrierConsumer(final TraverserSet<Edge> set) {
        if (executorService != null) {
            parallelBarrierConsumer(set);
            return;
        }
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());
        FireflyBatchReadHelper.pullFromLeft(traversal, graph, set, barrierSize);

        // Create output traverser set since we cant append to the input while we are iterating.
        final TraverserSet<Edge> output = new TraverserSet<>();

        // Info is used to keep track of how many output items we assign for each input (executed in order).
        final List<FireflyBatchReadHelper.ReadStepInfo<Edge>> fireflyBatchEdgeReadStepInfos = new ArrayList<>();
        final List<FireflyId> fireflyIdList = new ArrayList<>();
        final Set<FireflyId> uniqueIdSet = new HashSet<>();
        final Map<FireflyId, FireflyEdge> fireflyEdgeMap = new ConcurrentHashMap<>();
        final Map<Element, List<FireflyId>> duplicateIdMap = new HashMap<>();

        while (!set.isEmpty()) {
            // Get next input traverser and get the FireflyVertex form of it.
            final Traverser.Admin<Edge> traverser = set.remove();
            final FireflyVertex vertex = (FireflyVertex) traverser.get();

            // Latch the size of the current id list.
            final int previousSize = fireflyIdList.size();

            TraversalUtil.supernodeTraversalWarning(graph, this.traversal, vertex);
            // TODO GRAPH-1139: The entire iterator is consumed here and may OOM.
            if (duplicateIdMap.containsKey(vertex)) {
                fireflyIdList.addAll(duplicateIdMap.get(vertex));
            } else {
                final List<FireflyId> ids = new ArrayList<>();
                vertex.getBatchedEdgeIdsFromVertex(direction, edgeLabels, ids, aerospikeHasContainers);
                duplicateIdMap.put(vertex, ids);
                fireflyIdList.addAll(ids);
            }
            for (int i = previousSize; i < fireflyIdList.size(); i++) {
                final FireflyId id = fireflyIdList.get(i);
                if (!fireflyEdgeMap.containsKey(id)) {
                    uniqueIdSet.add(id);
                }
            }

            // Calculate how many ids were added by the function (size of list - previous size).
            // Create composite id info with this value and the appropriate traverser to the info list.
            fireflyBatchEdgeReadStepInfos.add(new FireflyBatchReadHelper.ReadStepInfo<>(traverser, fireflyIdList.size() - previousSize));

            // If we reach or exceed batch size then execute so we don't use too much memory at any given point. Also drain if list size gets very big.
            if (uniqueIdSet.size() >= graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE ||
                    fireflyIdList.size() >= 5 * graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE) {
                // Drain data to output. No need to pass in aerospikeHasContainers since they were used to filter Edge IDs already.
                FireflyBatchReadHelper.drainDataToOutput(this, fireflyIdList, uniqueIdSet,
                        fireflyEdgeMap, fireflyBatchEdgeReadStepInfos, Collections.emptyList(), fireflyHasContainers, output, graph::readEdges, null);
            }
        }

        // Drain data to output. No need to pass in aerospikeHasContainers since they were used to filter Edge IDs already.
        FireflyBatchReadHelper.drainDataToOutput(this, fireflyIdList, uniqueIdSet,
                fireflyEdgeMap, fireflyBatchEdgeReadStepInfos, Collections.emptyList(), fireflyHasContainers, output, graph::readEdges, null);

        if (output.isEmpty()) {
            set.add(EmptyTraverser.instance());
        } else {
            set.addAll(output);
            output.clear(); // Force garbage collection.
        }
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.direction, this.edgeLabels, this.barrierSize);
    }

    // A dumb hack because we lose exception context and don't know if it is checked or unchecked, so need to use this.
    public static void sneakyThrow(final ExecutionException e) {
        final Throwable t = e.getCause();
        if (t == null) sneakyThrowInternal(e); // Shouldn't happen, but if it does we want the context.
        sneakyThrowInternal(t);
    }

    @SuppressWarnings("unchecked")
    private static <T extends Throwable> void sneakyThrowInternal(final Throwable t) throws T {
        throw (T) t; // unchecked throw
    }
}
