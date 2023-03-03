package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.io.impl.relational.RelationalVertex;
import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyBatchEdgeReadStep extends CollectingBarrierStep<Edge> {
    private final Direction direction;
    private final String[] edgeLabels;

    // Set max barrier size so that if we get a really long running traversal that has output limit
    // the semi-lazy execution will allow the traversal to exit early.
    private static final int MAX_BARRIER_SIZE = 1000;
    public final List<HasContainer> fireflyHasContainers;
    public final List<HasContainer> aerospikeHasContainers;

    public FireflyBatchEdgeReadStep(final Traversal.Admin traversal,
                                    final Direction direction,
                                    final String[] edgeLabels,
                                    final Set<String> labels,
                                    final List<HasContainer> hasContainers) {
        super(traversal, MAX_BARRIER_SIZE);
        this.direction = direction;
        this.edgeLabels = edgeLabels;
        this.labels = labels;
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
        // Create output traverser set since we cant append to the input while we are iterating.
        final TraverserSet<Edge> output = new TraverserSet<>();
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());

        // Info is used to keep track of how many output items we assign for each input (executed in order).
        final List<FireflyBatchReadHelper.ReadStepInfo<Edge>> fireflyBatchEdgeReadStepInfos = new ArrayList<>();
        final List<FireflyId> fireflyIdList = new ArrayList<>();
        final Set<FireflyId> uniqueIdSet = new HashSet<>();
        final Map<FireflyId, FireflyEdge> fireflyEdgeMap = new TreeMap<>();
        while (!set.isEmpty()) {
            // Get next input traverser and get the RelationalVertex form of it.
            final Traverser.Admin<Edge> traverser = set.remove();
            final RelationalVertex vertex = (RelationalVertex) traverser.get();

            // Latch the size of the current id list.
            final int previousSize = fireflyIdList.size();

            // If the in edge cache is disabled then we need to use the regular interface.
            if (vertex.isEdgeCacheDisabled()) {
                if (direction == Direction.IN || direction == Direction.BOTH) {
                    final List<FireflyId> edgeIds = getEdgeIdsFromVertex(Direction.IN, graph, vertex);
                    FireflyBatchReadHelper.addElementsToSet(fireflyIdList, uniqueIdSet, fireflyEdgeMap, edgeIds);
                }
                if (direction == Direction.OUT || direction == Direction.BOTH) {
                    final List<FireflyId> edgeIds = getEdgeIdsFromVertex(Direction.OUT, graph, vertex);
                    FireflyBatchReadHelper.addElementsToSet(fireflyIdList, uniqueIdSet, fireflyEdgeMap, edgeIds);
                }
            } else {
                // Get the ids of the adjacent vertices and add them to the list.
                final List<FireflyId> edgeIds = new ArrayList<>();
                vertex.appendEdgeIds(edgeIds, direction, edgeLabels);
                FireflyBatchReadHelper.addElementsToSet(fireflyIdList, uniqueIdSet, fireflyEdgeMap, edgeIds);
            }

            // Calculate how many ids were added by the function (size of list - previous size).
            // Create composite id info with this value and the appropriate traverser to the info list.
            fireflyBatchEdgeReadStepInfos.add(new FireflyBatchReadHelper.ReadStepInfo<>(traverser, fireflyIdList.size() - previousSize));

            // If we reach or exceed batch size then execute so we don't use too much memory at any given point. Also drain if list size gets very big.
            if (uniqueIdSet.size() >= graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE ||
                    fireflyIdList.size() >= 5 * graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE) {
                // Drain data to output.
                FireflyBatchReadHelper.drainDataToOutput(this, fireflyIdList, uniqueIdSet,
                        fireflyEdgeMap, fireflyBatchEdgeReadStepInfos, aerospikeHasContainers, fireflyHasContainers, output, graph::readEdges);
            }
        }

        // Drain data to output.
        FireflyBatchReadHelper.drainDataToOutput(this, fireflyIdList, uniqueIdSet,
                fireflyEdgeMap, fireflyBatchEdgeReadStepInfos, aerospikeHasContainers, fireflyHasContainers, output, graph::readEdges);

        // Note this cannot be added in the above loop since we are looking through it above.
        set.addAll(output);
        output.clear(); // Force garbage collection.
    }

    private List<FireflyId> getEdgeIdsFromVertex(final Direction direction, final FireflyGraph firefly, final FireflyVertex vertex) {
        final List<FireflyId> edgeIds = vertex.getEdgeIdsFromVertex(direction);
        final Set<String> edgeLabelSet = Set.of(edgeLabels);
        // Note use aerospikeHasContainers here, those are to be applied on edges, so they are valid.
        return firefly.readEdges(aerospikeHasContainers, edgeIds).stream().filter(edge -> edgeLabels.length == 0 || edgeLabelSet.contains(edge.label())).map(e -> e.id).collect(Collectors.toList());
    }
}
