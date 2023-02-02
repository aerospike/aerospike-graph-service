package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.io.impl.relational.RelationalVertex;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
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

    // Set max barrier size so that if we get a really long running traversal that has output limit
    // the semi-lazy execution will allow the traversal to exit early.
    private static final int MAX_BARRIER_SIZE = 1000;

    public FireflyCompositeIdStep(final Traversal.Admin traversal,
                                  final Direction direction,
                                  final String[] edgeLabels,
                                  final Set<String> labels) {
        super(traversal, MAX_BARRIER_SIZE);
        this.direction = direction;
        this.edgeLabels = edgeLabels;
        this.labels = labels;
    }

    static class FireflyCompositeIdStepInfo {
        final Traverser.Admin<Vertex> traverser;
        final Integer size;

        public FireflyCompositeIdStepInfo(final Traverser.Admin<Vertex> traversers, final Integer size) {
            this.traverser = traversers;
            this.size = size;
        }
    }

    @Override
    public void barrierConsumer(final TraverserSet<Vertex> set) {
        // Create output traverser set since we cant append to the input while we are iterating.
        final TraverserSet<Vertex> output = new TraverserSet<>();
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());

        // Info is used to keep track of how many output items we assign for each input (executed in order).
        final List<FireflyCompositeIdStepInfo> fireflyCompositeIdStepInfos = new ArrayList<>();
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
            if (vertex.isEdgeCacheDisabled()) {
                if (direction == Direction.IN || direction == Direction.BOTH) {
                    final List<FireflyId> vertexIds = getVertexIdsFromEdges(Direction.IN, graph, vertex);
                    addVerticesToSet(fireflyIdList, uniqueIdSet, fireflyVertexMap, vertexIds);
                }
                if (direction == Direction.OUT || direction == Direction.BOTH) {
                    final List<FireflyId> vertexIds = getVertexIdsFromEdges(Direction.OUT, graph, vertex);
                    addVerticesToSet(fireflyIdList, uniqueIdSet, fireflyVertexMap, vertexIds);
                }
            } else {
                // Get the ids of the adjacent vertices and add them to the list.
                final List<FireflyId> vertexIds = new ArrayList<>();
                vertex.appendAdjacentVertexIds(vertexIds, direction, edgeLabels);
                addVerticesToSet(fireflyIdList, uniqueIdSet, fireflyVertexMap, vertexIds);
            }

            // Calculate how many ids were added by the function (size of list - previous size).
            // Create composite id info with this value and the appropriate traverser to the info list.
            fireflyCompositeIdStepInfos.add(new FireflyCompositeIdStepInfo(traverser, fireflyIdList.size() - previousSize));

            // If we reach or exceed batch size then execute so we don't use too much memory at any given point. Also drain if list size gets very big.
            if (uniqueIdSet.size() >= graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE ||
                    fireflyIdList.size() >= 5 * graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE) {
                // Drain data to output.
                drainDataToOutput(graph, fireflyIdList, uniqueIdSet, fireflyVertexMap, fireflyCompositeIdStepInfos, output);
            }
        }

        // Drain data to output.
        drainDataToOutput(graph, fireflyIdList, uniqueIdSet, fireflyVertexMap, fireflyCompositeIdStepInfos, output);

        // Note this cannot be added in the above loop since we are looking through it above.
        set.addAll(output);
        output.clear(); // Force garbage collection.
    }

    private List<FireflyId> getVertexIdsFromEdges(final Direction direction, final FireflyGraph firefly, final FireflyVertex vertex) {
        final List<FireflyId> edgeIds = vertex.getEdgeIdsFromVertex(direction);
        final Set<String> edgeLabelSet = Set.of(edgeLabels);
        List<FireflyId> results = (direction == Direction.IN) ?
                firefly.readEdges(edgeIds).stream().filter(edge -> edgeLabels.length == 0 || edgeLabelSet.contains(edge.label())).map(FireflyEdge::outVertexId).collect(Collectors.toList()) :
                firefly.readEdges(edgeIds).stream().filter(edge -> edgeLabels.length == 0 || edgeLabelSet.contains(edge.label())).map(FireflyEdge::inVertexId).collect(Collectors.toList());

        return results;
    }

    private void addVerticesToSet(List<FireflyId> fireflyIdList, Set<FireflyId> uniqueIdSet, Map<FireflyId, FireflyVertex> fireflyVertexMap, List<FireflyId> vertexIds) {
        fireflyIdList.addAll(vertexIds);
        for (final FireflyId id : vertexIds) {
            if (!fireflyVertexMap.containsKey(id)) {
                uniqueIdSet.add(id);
            }
        }
    }

    private void drainDataToOutput(final FireflyGraph firefly,
                       final List<FireflyId> fireflyIdList,
                       final Set<FireflyId> uniqueIdSet,
                       final Map<FireflyId, FireflyVertex> fireflyVertexMap,
                       final List<FireflyCompositeIdStepInfo> fireflyCompositeIdStepInfos,
                       final TraverserSet<Vertex> output) {
        // Read all vertices in a batch.
        final List<FireflyId> unorderedIds = new ArrayList<>(uniqueIdSet);
        final List<FireflyVertex> unorderedVertices = firefly.readVertices(unorderedIds);
        for (int i = 0; i < unorderedIds.size(); i++) {
            fireflyVertexMap.put(unorderedIds.get(i), unorderedVertices.get(i));
        }

        // Loop through the info list and assign the appropriate number of vertices to each traverser using the info.
        int i = 0;
        for (final FireflyCompositeIdStepInfo info : fireflyCompositeIdStepInfos) {
            for (int j = 0; j < info.size; j++) {
                // Create a new traverser with the vertex and add it to the output set using the split.
                // Note, this is invoked info.size times.
                FireflyId id = fireflyIdList.get(i++);
                final FireflyVertex fireflyVertex = fireflyVertexMap.get(id);
                output.add(info.traverser.split(fireflyVertex, this));
            }
        }

        // Clear intermediate buffers.
        fireflyIdList.clear();
        uniqueIdSet.clear();
        fireflyCompositeIdStepInfos.clear();
    }
}
