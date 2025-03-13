package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.process.traversal.step.util.TaskLogger;
import com.aerospike.firefly.process.traversal.step.util.TraversalUtil;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import io.vertx.core.impl.ConcurrentHashSet;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyBatchEdgeReadStep extends CollectingBarrierStep<Edge> implements LocalBarrier<Edge> {
    private final Direction direction;
    private final Set<String> edgeLabels;
    public final List<HasContainer> fireflyHasContainers;
    public final List<HasContainer> aerospikeHasContainers;
    private final int barrierSize;
    ExecutorService executorService;
    boolean debug = false;

    public FireflyBatchEdgeReadStep(final Traversal.Admin traversal,
                                    final Direction direction,
                                    final String[] edgeLabels,
                                    final Set<String> labels,
                                    final List<HasContainer> hasContainers,
                                    final int barrierSize) {
        super(traversal, barrierSize);
        ////TaskLogger.reset();
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
        final Map<String, Object> traversalOptions = new HashMap<>();
        traversal.getStrategies().
                getStrategy(OptionsStrategy.class).ifPresent(
                        optionsStrategy -> traversalOptions.putAll(optionsStrategy.getOptions()));
        if (traversalOptions.containsKey("aerospike.graph.parallelize")) {
            executorService = Executors.newFixedThreadPool(Integer.parseInt(traversalOptions.get("aerospike.graph.parallelize").toString()));
        } else {
            executorService = null;
        }
        if (traversalOptions.containsKey("aerospike.graph.debug")) {
            debug = Boolean.parseBoolean(traversalOptions.get("aerospike.graph.debug").toString());
        }
    }


    private void parallelConsumer(final TraverserSet<Edge> set) {
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());
        if (debug) {
            TaskLogger.reset();
        }
        FireflyBatchReadHelper.pullFromLeft(traversal, graph, set, barrierSize);
        if (debug) {
            TaskLogger.complete("pull");
        }

        // Create output traverser set since we cant append to the input while we are iterating.
        final List<TraverserSet<Edge>> output = new ArrayList<>();

        // Info is used to keep track of how many output items we assign for each input (executed in order).
        List<FireflyBatchReadHelper.ReadStepInfo<Edge>> fireflyBatchEdgeReadStepInfos = new ArrayList<>();
        List<FireflyId> fireflyIdList = new ArrayList<>();
        Set<FireflyId> uniqueIdSet = new ConcurrentHashSet<>();
        final Map<FireflyId, FireflyEdge> fireflyEdgeMap = new ConcurrentHashMap<>();
        final Map<Element, List<FireflyId>> duplicateIdMap = new HashMap<>();
        final Map<Element, Future<List<FireflyId>>> futures = new HashMap<>();
        final List<Pair<Element, Traverser.Admin>> orderedElements = new ArrayList<>();


        while (!set.isEmpty()) {
            // Get next input traverser and get the FireflyVertex form of it.
            final Traverser.Admin<Edge> traverser = set.remove();
            final FireflyVertex vertex = (FireflyVertex) traverser.get();
            orderedElements.add(new Pair<>(vertex, traverser));

            // Latch the size of the current id list.
            TraversalUtil.supernodeTraversalWarning(graph, this.traversal, vertex);
            if (!futures.containsKey(vertex)) {
                futures.put(vertex, executorService.submit(() -> {
                    final List<FireflyId> ids = new ArrayList<>();
                    vertex.getBatchedEdgeIdsFromVertex(direction, edgeLabels, ids, aerospikeHasContainers, fireflyEdgeMap);
                    return ids;
                }));
            }
        }
        if (debug) {
            TaskLogger.complete("launch");
        }

        List<Future<?>> futures2 = new ArrayList<>();
        for (final Pair<Element, Traverser.Admin> pair : orderedElements) {
            final Vertex element = (Vertex) pair.getValue0();
            final Traverser.Admin traverser = pair.getValue1();
            final int previousSize = fireflyIdList.size();
            try {
                if (duplicateIdMap.containsKey(element) && duplicateIdMap.get(element) != null) {
                    fireflyIdList.addAll(duplicateIdMap.get(element));
                } else {
                    final List<FireflyId> ids = futures.get(element).get();
                    fireflyIdList.addAll(ids);
                    duplicateIdMap.put(element, ids);
                }
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
            if (debug) {
                TaskLogger.complete("awaitIndex");
            }
            for (int i = previousSize; i < fireflyIdList.size(); i++) {
                final FireflyId id = fireflyIdList.get(i);
                if (!fireflyEdgeMap.containsKey(id)) {
                    uniqueIdSet.add(id);
                }
            }
            fireflyBatchEdgeReadStepInfos.add(new FireflyBatchReadHelper.ReadStepInfo<>(traverser, fireflyIdList.size() - previousSize));
            if (uniqueIdSet.size() >= graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE ||
                    fireflyIdList.size() >= 5 * graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE) {
                //System.out.println("Draining " + output.size());
                final TraverserSet<Edge> currentOutput = new TraverserSet<>();
                output.add(currentOutput);
                final List<FireflyId> ids = fireflyIdList;
                final Set<FireflyId> uniqueIds = uniqueIdSet;
                fireflyIdList = new ArrayList<>();
                uniqueIdSet = new HashSet<>();
                final List<FireflyBatchReadHelper.ReadStepInfo<Edge>> infos = fireflyBatchEdgeReadStepInfos;
                fireflyBatchEdgeReadStepInfos = new ArrayList<>();
                futures2.add(executorService.submit(() -> {
                    // Drain data to output. No need to pass in aerospikeHasContainers since they were used to filter Edge IDs already.
                    FireflyBatchReadHelper.drainDataToOutput(this, ids, uniqueIds,
                            fireflyEdgeMap, infos, Collections.emptyList(), fireflyHasContainers, currentOutput, graph::readEdges, null);
                    return null;
                }));
            }
            if (debug) {
                TaskLogger.complete("submit2");
            }
        }
        if (debug) {
            TaskLogger.complete("batchRead");
        }

        // Drain data to output. No need to pass in aerospikeHasContainers since they were used to filter Edge IDs already.
        final TraverserSet<Edge> currentOutput = new TraverserSet<>();
        output.add(currentOutput);
        final List<FireflyId> ids = fireflyIdList;
        final Set<FireflyId> uniqueIds = uniqueIdSet;
        final List<FireflyBatchReadHelper.ReadStepInfo<Edge>> infos = new ArrayList<>(fireflyBatchEdgeReadStepInfos);
        futures2.add(executorService.submit(() -> {
            FireflyBatchReadHelper.drainDataToOutput(this, ids, uniqueIds,
                    fireflyEdgeMap, infos, Collections.emptyList(), fireflyHasContainers, currentOutput, graph::readEdges, null);
            return null;
        }));
        for (final Future<?> future22 : futures2) {
            try {
                future22.get();
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        }
        if (debug) {
            TaskLogger.complete("awaitFutures2");
        }

        //TaskLogger.complete("drain");

        final TraverserSet<Edge> finalOutput = new TraverserSet<>();
        for (final TraverserSet<Edge> traverserSet : output) {
            finalOutput.addAll(traverserSet);
            traverserSet.clear();
        }
        if (finalOutput.isEmpty()) {
            set.add(EmptyTraverser.instance());
        } else {
            set.addAll(finalOutput);
            output.clear(); // Force garbage collection.
        }
        if (debug) {
            TaskLogger.complete("complete");
            TaskLogger.log(graph);
        }
    }

    @Override
    public void barrierConsumer(final TraverserSet<Edge> set) {
        if (executorService != null) {
            parallelConsumer(set);
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
        final Set<Element> input = new HashSet<>();

        for (final Traverser.Admin<Edge> e : set) {
            if (input.contains(e.get())) {
                duplicateIdMap.putIfAbsent(e.get(), null);
            } else {
                input.add(e.get());
            }
        }

        if (set.size() != input.size()) {
            System.out.println("Set size: " + set.size() + " Input size: " + input.size());
        }

        //TaskLogger.complete("start");
        while (!set.isEmpty()) {
            // Get next input traverser and get the FireflyVertex form of it.
            final Traverser.Admin<Edge> traverser = set.remove();
            final FireflyVertex vertex = (FireflyVertex) traverser.get();

            // Latch the size of the current id list.
            final int previousSize = fireflyIdList.size();
            TraversalUtil.supernodeTraversalWarning(graph, this.traversal, vertex);

            if (duplicateIdMap.containsKey(vertex) && duplicateIdMap.get(vertex) != null) {
                fireflyIdList.addAll(duplicateIdMap.get(vertex));
            } else {
                // TODO GRAPH-1139: The entire iterator is consumed here and may OOM.
                vertex.getBatchedEdgeIdsFromVertex(direction, edgeLabels, fireflyIdList, aerospikeHasContainers, fireflyEdgeMap);
                if (!duplicateIdMap.containsKey(vertex)) {
                    final List<FireflyId> subList = new ArrayList<>(fireflyIdList.subList(previousSize, fireflyIdList.size()));
                    duplicateIdMap.put(vertex, subList);
                }
            }
            //TaskLogger.complete("index");

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

        //TaskLogger.complete("drain");

        if (output.isEmpty()) {
            set.add(EmptyTraverser.instance());
        } else {
            set.addAll(output);
            output.clear(); // Force garbage collection.
        }

        //TaskLogger.complete("assign");

        //TaskLogger.log(graph);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.direction, this.edgeLabels, this.barrierSize);
    }
}
