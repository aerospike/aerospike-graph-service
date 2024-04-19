package com.aerospike.firefly.process.traversal.step.map;

import com.aerospike.firefly.process.traversal.step.util.TraversalUtil;
import com.aerospike.firefly.structure.FireflyVertex;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class FireflyVertexEdgeLocalCountStep extends MapStep<Vertex, Long> {
    private final Direction direction;

    public FireflyVertexEdgeLocalCountStep(final Traversal.Admin traversal, final Direction direction,
                                           final Set<String> labels) {
        super(traversal);
        this.direction = direction;
        this.labels = labels;
    }

    @Override
    protected Traverser.Admin<Long> processNextStart() throws NoSuchElementException {
        final Traverser.Admin<Vertex> traverser = this.starts.next();
        final FireflyVertex vertex = (FireflyVertex) traverser.get();
        TraversalUtil.supernodeTraversalWarning(this.traversal, vertex);
        return traverser.split(vertex.getEdgeCount(direction), this);
    }
}
