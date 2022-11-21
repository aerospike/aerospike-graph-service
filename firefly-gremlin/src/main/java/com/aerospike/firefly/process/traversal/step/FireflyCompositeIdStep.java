package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.io.impl.relational.RelationalVertex;
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
import java.util.List;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyCompositeIdStep extends CollectingBarrierStep<Vertex> {
    private final Direction direction;
    private final String[] edgeLabels;

    public FireflyCompositeIdStep(final Traversal.Admin traversal,
                                  final Direction direction,
                                  final String[] edgeLabels) {
        super(traversal);
        this.direction = direction;
        this.edgeLabels = edgeLabels;
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
        while (!set.isEmpty()) {
            // Get next input traverser and get the RelationalVertex form of it.
            final Traverser.Admin<Vertex> traverser = set.remove();
            final RelationalVertex vertex = (RelationalVertex) traverser.get();

            // Latch the size of the current id list.
            final int previousSize = fireflyIdList.size();

            // Get the ids of the adjacent vertices and add them to the list.
            vertex.appendAdjacentVertexIds(fireflyIdList, direction, edgeLabels);

            // Calculate how many ids were added by the function (size of list - previous size).
            // Create composite id info with this value and the appropriate traverser to the info list.
            fireflyCompositeIdStepInfos.add(new FireflyCompositeIdStepInfo(traverser, fireflyIdList.size() - previousSize));
        }

        // Read all vertices in a batch.
        final List<FireflyVertex> vertices = firefly.readVertices(fireflyIdList);

        // Loop through the info list and assign the appropriate number of vertices to each traverser using the info.
        int i = 0;
        for (final FireflyCompositeIdStepInfo info : fireflyCompositeIdStepInfos) {
            for (int j = 0; j < info.size; j++) {
                // Create a new traverser with the vertex and add it to the output set using the split.
                // Not this is invoked info.size times,
                output.add(info.traverser.split(vertices.get(i++), this));
            }
        }

        // This should never happen, but just in case the above logic gets changes and it breaks the code,
        // an exception is thrown to prevent a silent failure.
        if (i != vertices.size()) {
            throw new RuntimeException("Something went wrong " + i + " != " + vertices.size());
        }

        // Note this cannot be added in the above loop since we are looking through it above.
        set.addAll(output);
        output.clear(); // Force garbage collection.
    }
}
