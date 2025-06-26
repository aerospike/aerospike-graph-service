package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.List;
import java.util.Set;

public class FireflyEdgeToVertexBatchReadStep extends VertexBatchReadStep {

    private final Direction direction;

    public FireflyEdgeToVertexBatchReadStep(final Traversal.Admin traversal,
                                            final Direction direction,
                                            final List<HasContainer> hasContainers,
                                            final Set<String> labels,
                                            final int barrierSize,
                                            final boolean areEdgesRequired) {
        super(traversal, hasContainers, labels, barrierSize, areEdgesRequired);
        this.direction = direction;
    }

    @Override
    protected List<FireflyId> getVertexIds(final Traverser.Admin traverser) {
        final FireflyEdge edge = (FireflyEdge) traverser.get();

        switch (direction) {
            case OUT:
                return List.of(edge.outVertexId());
            case IN:
                return List.of(edge.inVertexId());
            default:
                return List.of(edge.outVertexId(), edge.inVertexId());
        }
    }
}
