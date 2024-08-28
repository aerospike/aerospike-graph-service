package com.aerospike.firefly.process.traversal.step.map;

import com.aerospike.firefly.structure.FireflyVertex;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;
import java.util.Set;

public class FireflyAdjacentVertexIdStep extends FlatMapStep<Vertex, Object> implements AutoCloseable {
    private final String[] edgeLabels;
    private Direction direction;

    public FireflyAdjacentVertexIdStep(final Traversal.Admin traversal,
                                       final Set<String> labels,
                                       final Direction direction,
                                       final String... edgeLabels) {
        super(traversal);
        this.labels = labels;
        this.direction = direction;
        this.edgeLabels = edgeLabels;
    }

    @Override
    protected Iterator<Object> flatMap(final Traverser.Admin<Vertex> traverser) {
        // TODO: Implement this method
        final FireflyVertex vertex = (FireflyVertex) traverser.get();
        // return vertex.adjacentVertexIds(this.direction, this.edgeLabels).iterator();
        return null;
    }

    @Override
    public void close() throws Exception {
        closeIterator();
    }
}
