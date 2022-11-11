package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.RelationalVertex;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyCompositeIdStep extends AbstractStep {
    private final Direction direction;
    private final String[] edgeLabels;
    private final FireflyGraph graph;

    public FireflyCompositeIdStep(final Traversal.Admin traversal,
                                  FireflyGraph graph,
                                  Direction direction,
                                  String[] ... edgeLabels) {
        super(traversal);
        this.direction = direction;
        this.edgeLabels = edgeLabels[0];
        this.graph = graph;
    }

    @Override
    protected Traverser.Admin processNextStart() throws NoSuchElementException {
        System.out.println("Composite id step");
        if (starts.hasNext()) {
            System.out.println("Composite id step has next");
        } else {
            System.out.println("Composite id step has no next");
        }
        List<FireflyId> fireflyIdList = new ArrayList<>();
        while (starts.hasNext()) {
            Traverser.Admin<?> next = starts.next();
            RelationalVertex vertex = (RelationalVertex) next.get();
                vertex.appendAdjacentVertexIds(fireflyIdList, direction, edgeLabels);
        }
        List<FireflyVertex> vertices = graph.readVertices(fireflyIdList);

        return null;
    }
}
