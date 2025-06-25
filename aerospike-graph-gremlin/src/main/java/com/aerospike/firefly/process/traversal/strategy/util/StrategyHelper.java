package com.aerospike.firefly.process.traversal.strategy.util;

import com.aerospike.firefly.process.traversal.step.FireflyBatchVertexReadSampleLimitStep;
import com.aerospike.firefly.process.traversal.step.FireflyBatchVertexReadStep;
import com.aerospike.firefly.process.traversal.step.map.FireflyCountGlobalLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.ArrayList;
import java.util.List;

public class StrategyHelper {
    public static boolean requiresEdges(final Traversal.Admin<?, ?> traversal, final List<Step> steps, final int startIndex) {
        if (traversal.isRoot()) {
            return requiresEdges(steps, startIndex);
        } else {
            // steps after parent can use in/out edges
            final Step parentStep = traversal.getParent().asStep();
            final Traversal.Admin parentTraversal = parentStep.getTraversal().asAdmin();
            final List<Step> parentSteps = parentTraversal.getSteps();
            return requiresEdges(steps, startIndex) || requiresEdges(parentTraversal, parentSteps, parentSteps.indexOf(parentStep));
        }
    }

    private static boolean requiresEdges(final List<Step> steps, final int startIndex) {
        for (int i = startIndex; i < steps.size(); i++) {
            if (steps.get(i) instanceof VertexStep || steps.get(i) instanceof EdgeVertexStep
                    || steps.get(i) instanceof FireflyCountGlobalLocalStep
                    || steps.get(i) instanceof FireflyBatchVertexReadStep
                    || steps.get(i) instanceof FireflyBatchVertexReadSampleLimitStep
                    // let's play as safe as possible with repeat step
                    || steps.get(i) instanceof RepeatStep.RepeatEndStep ) {
                return true;
            }
            if (steps.get(i) instanceof TraversalParent) {
                final List<Traversal.Admin<Object, Object>> children = new ArrayList<>(((TraversalParent) steps.get(i)).getLocalChildren());
                // for steps like match
                children.addAll(((TraversalParent) steps.get(i)).getGlobalChildren());
                for (final Traversal.Admin child : children) {
                    if (TraversalHelper.hasStepOfAssignableClassRecursively(VertexStep.class, child)
                            || TraversalHelper.hasStepOfAssignableClassRecursively(EdgeVertexStep.class, child)
                            || TraversalHelper.hasStepOfAssignableClassRecursively(FireflyCountGlobalLocalStep.class, child)
                            || TraversalHelper.hasStepOfAssignableClassRecursively(FireflyBatchVertexReadStep.class, child)
                            || TraversalHelper.hasStepOfAssignableClassRecursively(FireflyBatchVertexReadSampleLimitStep.class, child)
                            || TraversalHelper.hasStepOfAssignableClassRecursively(RepeatStep.RepeatEndStep.class, child)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }
}
