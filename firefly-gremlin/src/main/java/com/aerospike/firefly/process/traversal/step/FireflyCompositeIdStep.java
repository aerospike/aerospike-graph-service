package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.RelationalVertex;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LambdaCollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ExpandableStepIterator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

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

    //static class CompositeStepInfo {
    //    RelationalVertex start;

    //    List<? extends Vertex> end
    //}

    @Override
    public void barrierConsumer(final TraverserSet<Vertex> set) {
        final TraverserSet<Vertex> output = new TraverserSet<>();
        final FireflyGraph firefly = ((FireflyGraph) getTraversal().getGraph().get());
        final List<FireflyId> fireflyIdList = new ArrayList<>();
        while (!set.isEmpty()) {
            final Traverser.Admin<Vertex> traverser = set.remove();
            final RelationalVertex vertex = (RelationalVertex) traverser.get();
            vertex.appendAdjacentVertexIds(fireflyIdList, direction, edgeLabels);
            final List<? extends Vertex> vertices = firefly.readVertices(fireflyIdList);
            vertices.forEach(v -> output.add(traverser.split(v, this)));
        }

        // Note this cannot be added in the above loop since we are looking through it above.
        set.addAll(output);
        output.clear(); // Force garbage collection.
    }
}
