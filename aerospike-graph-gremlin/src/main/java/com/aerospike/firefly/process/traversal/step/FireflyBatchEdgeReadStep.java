package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.process.traversal.step.util.TaskLogger;
import com.aerospike.firefly.process.traversal.step.util.TraversalUtil;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

    public FireflyBatchEdgeReadStep(final Traversal.Admin traversal,
                                    final Direction direction,
                                    final String[] edgeLabels,
                                    final Set<String> labels,
                                    final List<HasContainer> hasContainers,
                                    final int barrierSize) {
        super(traversal, barrierSize);
        TaskLogger.reset();
        this.direction = direction;
        this.edgeLabels = new HashSet<>(Arrays.asList(edgeLabels));
        this.labels = new HashSet<>(labels);
        this.barrierSize = barrierSize;
        if (hasContainers != null) {
            final List<FireflyGraphStep.HasContainerWithCardinality> hasContainerWithCardinalities =
                    FireflyBatchReadHelper.getHasContainersWithCardinalityOrder((FireflyGraph) getTraversal().getGraph().get(), Vertex.class, hasContainers);
            // TODO GRAPH-401: This is a hack to get around the fact that we cannot filter our cache with a hasContainer.
            //  To get around this we have to filter everything post read again, so all containers pushed to firefly no
            //  matter what.
            fireflyHasContainers = hasContainerWithCardinalities.stream().map(a -> a.hasContainer).collect(Collectors.toList());
            aerospikeHasContainers = FireflyBatchReadHelper.getAerospikeHasContainers(hasContainerWithCardinalities);
        } else {
            fireflyHasContainers = List.of();
            aerospikeHasContainers = List.of();
        }
    }

    @Override
    public void barrierConsumer(final TraverserSet<Edge> set) {
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());
        FireflyBatchReadHelper.pullFromLeft(traversal, graph, set, barrierSize);

        // Create output traverser set since we cant append to the input while we are iterating.
        final TraverserSet<Edge> output = new TraverserSet<>();

        // Info is used to keep track of how many output items we assign for each input (executed in order).
        final List<FireflyBatchReadHelper.ReadStepInfo<Edge>> fireflyBatchEdgeReadStepInfos = new ArrayList<>();
        final List<FireflyId> fireflyIdList = new ArrayList<>();
        final Set<FireflyId> uniqueIdSet = new HashSet<>();
        final Map<FireflyId, FireflyEdge> fireflyEdgeMap = new HashMap<>();
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

        TaskLogger.complete("start");
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
                vertex.getBatchedEdgeIdsFromVertex(direction, edgeLabels, fireflyIdList, aerospikeHasContainers);
                if (duplicateIdMap.containsKey(vertex)) {
                    final List<FireflyId> subList = new ArrayList<>(fireflyIdList.subList(previousSize, fireflyIdList.size()));
                    duplicateIdMap.put(vertex, subList);
                }
            }
            TaskLogger.complete("index");

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

        TaskLogger.complete("drain");

        if (output.isEmpty()) {
            set.add(EmptyTraverser.instance());
        } else {
            set.addAll(output);
            output.clear(); // Force garbage collection.
        }

        TaskLogger.complete("assign");

        TaskLogger.log(graph);
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.direction, this.edgeLabels, this.barrierSize);
    }
}
