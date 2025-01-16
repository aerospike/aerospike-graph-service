package com.aerospike.firefly.process.computer.util;

import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ComputerHelper {
    public static boolean onGraphComputer(Traversal.Admin<?, ?> traversal) {
        while (!(traversal.isRoot())) {
            if (traversal.getParent() instanceof TraversalVertexProgramStep)
                return true;
            traversal = traversal.getParent().asStep().getTraversal();
        }
        if (traversal.getSteps().size() > 0) {
            return traversal.getSteps().get(0) instanceof TraversalVertexProgramStep;
        } else {
            return false;
        }
    }

    public static List<HasContainer> getInitialHasContainers(final Traversal.Admin<?, ?> traversal) {
        final List<HasContainer> hasContainers = new ArrayList<>();
        if (traversal.getStartStep() instanceof GraphStep && ((GraphStep<Vertex, Vertex>) traversal.getStartStep()).returnsVertex()) {
            if (Stream.of(((GraphStep) traversal.getStartStep()).getIds()).count() > 0)
                hasContainers.add(new HasContainer(T.id.getAccessor(), P.eq(P.within(((GraphStep) traversal.getStartStep()).getIds()))));
            for (Step<?, ?> currentStep = ((GraphStep) traversal.getStartStep()).getNextStep();
                 currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep || currentStep instanceof ProfileStep;
                 currentStep = currentStep.getNextStep()) {
                if (currentStep instanceof HasStep) {
                    if (((HasStep) currentStep).getHasContainers().stream().filter(it -> (((HasContainer) it).getKey() == null)).findAny().isPresent()) {
                        for (HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
                            hasContainers.add(hasContainer);
                        }
                    } else {
                        for (final HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers().stream()
                                .filter(h -> h.getKey().equals(T.id.getAccessor()) || h.getValue() instanceof Number || h.getValue() instanceof Number ||
                                        (h.getPredicate().getPredicateName().equals(P.eq(1).getPredicateName()))).collect(Collectors.toList())) {
                            hasContainers.add(hasContainer);
                        }
                    }
                }
            }
        }
        return hasContainers;
    }
}
