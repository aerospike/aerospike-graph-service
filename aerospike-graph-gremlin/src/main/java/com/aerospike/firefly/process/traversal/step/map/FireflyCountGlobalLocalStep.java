package com.aerospike.firefly.process.traversal.step.map;

import com.aerospike.firefly.structure.FireflyVertex;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.util.function.ConstantSupplier;

import java.util.Set;
import java.util.function.BinaryOperator;

public class FireflyCountGlobalLocalStep<S> extends ReducingBarrierStep<S, Long> {
    private final Direction direction;
    private final String[] edgeLabels;

    public FireflyCountGlobalLocalStep(final Traversal.Admin traversal,
                                       final Direction direction,
                                       final Set<String> labels,
                                       final String[] edgeLabels) {
        super(traversal);
        this.setSeedSupplier(new ConstantSupplier<>(0L));
        this.setReducingBiOperator((BinaryOperator) Operator.sumLong);
        this.direction = direction;
        this.labels = labels;
        this.edgeLabels = edgeLabels;
    }

    @Override
    public Long projectTraverser(final Traverser.Admin<S> traverser) {
        final S element = traverser.get();
        final FireflyVertex fireflyVertex;
        if (element instanceof ComputerGraph.ComputerVertex) {
            fireflyVertex = (FireflyVertex) ((ComputerGraph.ComputerVertex) element).getBaseVertex();
        } else {
            fireflyVertex = (FireflyVertex) element;
        }
        System.out.println("FireflyCountGlobalLocalStep.projectTraverser for " + traverser
                + " returns " + fireflyVertex.getEdgeCount(direction, edgeLabels));
        return fireflyVertex.getEdgeCount(direction, edgeLabels) * traverser.bulk();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Set.of();
    }
}
