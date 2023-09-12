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
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ExpandableStepIterator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
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
public class FireflyCompositeIdStep extends CollectingBarrierStep<Vertex> {
    private final Direction direction;
    private final String[] edgeLabels;

    // Set max barrier size so that if we get a really long-running traversal that has output limit
    // the semi-lazy execution will allow the traversal to exit early.
    private static final int MAX_BARRIER_SIZE = 1000;

    // HasContainers to apply to the read of the composite id step to filter results.
    public final List<HasContainer> fireflyHasContainers;
    public final List<HasContainer> aerospikeHasContainers;

    public FireflyCompositeIdStep(final Traversal.Admin traversal,
                                  final Direction direction,
                                  final String[] edgeLabels,
                                  final Set<String> labels,
                                  final List<HasContainer> hasContainers) {
        super(traversal, MAX_BARRIER_SIZE);
        this.direction = direction;
        this.edgeLabels = edgeLabels;
        this.labels = new HashSet<>(labels);
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
    public void barrierConsumer(final TraverserSet<Vertex> set) {
        // Create output traverser set since we cant append to the input while we are iterating.
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());
        if (traversal.getParent() instanceof RepeatStep) {
            if (graph.getBaseGraph().ENABLE_BATCHED_REPEAT_STEP_STRATEGY) {
                final RepeatStep repeatStep = (RepeatStep) traversal.getParent();
                final ExpandableStepIterator repeatStarts = repeatStep.getStarts();
                while (repeatStarts.hasNext() && set.size() < MAX_BARRIER_SIZE) {
                    set.add(repeatStarts.next());
                }
            }
        }

        final TraverserSet<Vertex> output = new TraverserSet<>();

        // Info is used to keep track of how many output items we assign for each input (executed in order).
        final List<FireflyBatchReadHelper.ReadStepInfo<Vertex>> fireflyCompositeIdStepInfos = new ArrayList<>();
        final List<FireflyId> fireflyIdList = new ArrayList<>();
        final Set<FireflyId> uniqueIdSet = new HashSet<>();
        final Map<FireflyId, FireflyVertex> fireflyVertexMap = new TreeMap<>();
        while (!set.isEmpty()) {
            // Get next input traverser and get the RelationalVertex form of it.
            final Traverser.Admin<Vertex> traverser = set.remove();
            final RelationalVertex vertex = (RelationalVertex) traverser.get();

            // Latch the size of the current id list.
            final int previousSize = fireflyIdList.size();

            // If the in edge cache is disabled then we need to use the regular interface.
            if (vertex.isEdgeCacheOverflowed()) {
                if (direction == Direction.IN || direction == Direction.BOTH) {
                    final List<FireflyId> vertexIds = getVertexIdsFromEdges(Direction.IN, graph, vertex);
                    FireflyBatchReadHelper.addElementsToSet(fireflyIdList, uniqueIdSet, fireflyVertexMap, vertexIds);
                }
                if (direction == Direction.OUT || direction == Direction.BOTH) {
                    final List<FireflyId> vertexIds = getVertexIdsFromEdges(Direction.OUT, graph, vertex);
                    FireflyBatchReadHelper.addElementsToSet(fireflyIdList, uniqueIdSet, fireflyVertexMap, vertexIds);
                }
            } else {
                // Get the ids of the adjacent vertices and add them to the list.
                final List<FireflyId> vertexIds = new ArrayList<>();
                vertex.appendAdjacentVertexIds(vertexIds, direction, edgeLabels);
                FireflyBatchReadHelper.addElementsToSet(fireflyIdList, uniqueIdSet, fireflyVertexMap, vertexIds);
            }

            // Calculate how many ids were added by the function (size of list - previous size).
            // Create composite id info with this value and the appropriate traverser to the info list.
            fireflyCompositeIdStepInfos.add(new FireflyBatchReadHelper.ReadStepInfo<>(traverser, fireflyIdList.size() - previousSize));

            // If we reach or exceed batch size then execute so we don't use too much memory at any given point. Also drain if list size gets very big.
            if (uniqueIdSet.size() >= graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE ||
                    fireflyIdList.size() >= 5 * graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE) {
                // Drain data to output.
                FireflyBatchReadHelper.drainDataToOutput(this, fireflyIdList, uniqueIdSet,
                        fireflyVertexMap, fireflyCompositeIdStepInfos, aerospikeHasContainers, fireflyHasContainers, output, graph::readVertices);
            }
        }

        // Drain data to output.
        FireflyBatchReadHelper.drainDataToOutput(this, fireflyIdList, uniqueIdSet,
                fireflyVertexMap, fireflyCompositeIdStepInfos, aerospikeHasContainers, fireflyHasContainers, output, graph::readVertices);

        // Note this cannot be added in the above loop since we are looking through it above.
        set.addAll(output);
        output.clear(); // Force garbage collection.
    }

    private List<FireflyId> getVertexIdsFromEdges(final Direction direction, final FireflyGraph firefly, final FireflyVertex vertex) {
        final List<FireflyId> edgeIds = vertex.getEdgeIdsFromVertex(direction);
        final Set<String> edgeLabelSet = Set.of(edgeLabels);
        return (direction == Direction.IN) ?
                // Note do not use aerospikeHasContainers here, those are to be applied on vertices, not edges.
                firefly.readEdges(List.of(), edgeIds).stream().filter(edge -> edgeLabels.length == 0 || edgeLabelSet.contains(edge.label())).map(FireflyEdge::outVertexId).collect(Collectors.toList()) :
                firefly.readEdges(List.of(), edgeIds).stream().filter(edge -> edgeLabels.length == 0 || edgeLabelSet.contains(edge.label())).map(FireflyEdge::inVertexId).collect(Collectors.toList());
    }
}
