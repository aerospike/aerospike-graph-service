package com.aerospike.firefly.process.traversal.step.map;

import com.aerospike.firefly.structure.FireflyVertex;
import groovy.console.ui.SystemOutputInterceptor;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.util.function.ConstantSupplier;

import java.util.Set;
import java.util.function.BinaryOperator;

import static com.aerospike.firefly.process.computer.local.ComputerHelper.isComputerTraversal;

public class FireflyCountGlobalLocalStep<S> extends ReducingBarrierStep<S, Long> {
    final boolean isComputer;
    final Direction direction;

    public FireflyCountGlobalLocalStep(final Traversal.Admin traversal, final Direction direction, Set<String> labels) {
        super(traversal);
        this.setSeedSupplier(new ConstantSupplier<>(0L));
        this.setReducingBiOperator((BinaryOperator) Operator.sumLong);
        this.isComputer = isComputerTraversal(traversal);
        this.direction = direction;
        super.labels = labels;
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
        return fireflyVertex.getEdgeCount(direction);
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Set.of();
    }
}
