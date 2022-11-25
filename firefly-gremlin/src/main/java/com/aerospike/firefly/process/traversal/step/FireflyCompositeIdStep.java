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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyCompositeIdStep extends CollectingBarrierStep<Vertex> {
    private final Direction direction;
    private final String[] edgeLabels;

    public FireflyCompositeIdStep(final Traversal.Admin traversal,
                                  final Direction direction,
                                  final String[] edgeLabels,
                                  final Set<String> labels) {
        super(traversal);
        this.direction = direction;
        this.edgeLabels = edgeLabels;
        this.labels = labels;
    }

    static class FireflyCompositeIdStepInfo {
        final Traverser.Admin<Vertex> traverser;
        final Integer size;

        public FireflyCompositeIdStepInfo(Traverser.Admin<Vertex> traversers, Integer size) {
            this.traverser = traversers;
            this.size = size;
        }
    }

    @Override
    public void barrierConsumer(final TraverserSet<Vertex> set) {
        // Create output traverser set since we cant append to the input while we are iterating.
        final TraverserSet<Vertex> output = new TraverserSet<>();
        final FireflyGraph firefly = ((FireflyGraph) getTraversal().getGraph().get());

        // Info is used to keep track of how many output items we assign for each input (executed in order).
        final List<FireflyCompositeIdStepInfo> fireflyCompositeIdStepInfos = new ArrayList<>();
        final List<FireflyId> fireflyIdList = new ArrayList<>();
        final Set<FireflyId> uniqueIdSet = new HashSet<>();
        final Map<FireflyId, FireflyVertex> fireflyVertexMap = new HashMap<>();
        while (!set.isEmpty()) {
            // Get next input traverser and get the RelationalVertex form of it.
            final Traverser.Admin<Vertex> traverser = set.remove();
            final RelationalVertex vertex = (RelationalVertex) traverser.get();

            // Latch the size of the current id list.
            final int previousSize = fireflyIdList.size();

            // If the in edge count or out edge count is -1 (invalid) then we need to use regular interface.
            if (vertex.inEdgeCount == -1 || vertex.outEdgeCount == -1) {
                if (direction == Direction.IN || direction == Direction.BOTH) {
                    final List<FireflyId> edgeIds = vertex.getEdgeIdsFromVertex(Direction.IN);
                    final List<FireflyId> vertexIds = firefly.readEdges(edgeIds).stream().map(FireflyEdge::inVertexId).collect(Collectors.toList());
                    fireflyIdList.addAll(vertexIds);
                    for (FireflyId id : vertexIds) {
                        if (!fireflyVertexMap.containsKey(id)) {
                            uniqueIdSet.add(id);
                        }
                    }
                } else if (direction == Direction.OUT || direction == Direction.BOTH) {
                    final List<FireflyId> edgeIds = vertex.getEdgeIdsFromVertex(Direction.OUT);
                    final List<FireflyId> vertexIds = firefly.readEdges(edgeIds).stream().map(FireflyEdge::outVertexId).collect(Collectors.toList());
                    fireflyIdList.addAll(vertexIds);
                    for (FireflyId id : vertexIds) {
                        if (!fireflyVertexMap.containsKey(id)) {
                            uniqueIdSet.add(id);
                        }
                    }
                }
            } else {
                // Get the ids of the adjacent vertices and add them to the list.
                final List<FireflyId> vertexIds = new ArrayList<>();
                vertex.appendAdjacentVertexIds(vertexIds, direction, edgeLabels);
                fireflyIdList.addAll(vertexIds);
                for (FireflyId id : vertexIds) {
                    if (!fireflyVertexMap.containsKey(id)) {
                        uniqueIdSet.add(id);
                    }
                }
            }

            // Calculate how many ids were added by the function (size of list - previous size).
            // Create composite id info with this value and the appropriate traverser to the info list.
            fireflyCompositeIdStepInfos.add(new FireflyCompositeIdStepInfo(traverser, fireflyIdList.size() - previousSize));

            if (uniqueIdSet.size() >= firefly.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE ||
                    fireflyIdList.size() >= 5 * firefly.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE) {
                drainDataToOutput(firefly, fireflyIdList, uniqueIdSet, fireflyVertexMap, fireflyCompositeIdStepInfos, output);
            }
        }

        drainDataToOutput(firefly, fireflyIdList, uniqueIdSet, fireflyVertexMap, fireflyCompositeIdStepInfos, output);

        // Note this cannot be added in the above loop since we are looking through it above.
        set.addAll(output);
        output.clear(); // Force garbage collection.
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
        fireflyIdList.clear();
        uniqueIdSet.clear();
        fireflyCompositeIdStepInfos.clear();
    }
}
