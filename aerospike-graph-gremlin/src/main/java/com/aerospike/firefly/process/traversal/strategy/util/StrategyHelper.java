package com.aerospike.firefly.process.traversal.strategy.util;

import com.aerospike.firefly.process.traversal.step.FireflyBatchEdgeReadSampleLimitStep;
import com.aerospike.firefly.process.traversal.step.FireflyBatchEdgeReadStep;
import com.aerospike.firefly.process.traversal.step.FireflyBatchVertexReadSampleLimitStep;
import com.aerospike.firefly.process.traversal.step.FireflyBatchVertexReadStep;
import com.aerospike.firefly.process.traversal.step.FireflyEdgeToVertexBatchReadStep;
import com.aerospike.firefly.process.traversal.step.FireflyOtherVBatchReadStep;
import com.aerospike.firefly.process.traversal.step.computer.FireflyBatchEdgeReadStepLocal;
import com.aerospike.firefly.process.traversal.step.computer.FireflyBatchVertexReadStepLocal;
import com.aerospike.firefly.process.traversal.step.computer.FireflyOtherVBatchReadStepLocal;
import com.aerospike.firefly.process.traversal.step.map.FireflyCountGlobalLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;

import java.util.ArrayList;
import java.util.List;

public class StrategyHelper {
    public static boolean areEdgesRequired(final Traversal.Admin<?, ?> traversal, final List<Step> steps, final int startIndex) {
        if (traversal.isRoot()) {
            return areEdgesRequired(steps, startIndex);
        } else {
            // steps after parent can use in/out edges
            final Step parentStep = traversal.getParent().asStep();
            final Traversal.Admin parentTraversal = parentStep.getTraversal().asAdmin();
            final List<Step> parentSteps = parentTraversal.getSteps();
            return areEdgesRequired(steps, startIndex) || areEdgesRequired(parentTraversal, parentSteps, parentSteps.indexOf(parentStep));
        }
    }

    private static boolean areEdgesRequired(final List<Step> steps, final int startIndex) {
        for (int i = startIndex; i < steps.size(); i++) {
            // raw TinkerPop steps
            if (steps.get(i) instanceof VertexStep || steps.get(i) instanceof EdgeVertexStep
                    // already replaced FireFly steps
                    || steps.get(i) instanceof FireflyCountGlobalLocalStep
                    || steps.get(i) instanceof FireflyBatchVertexReadStep
                    || steps.get(i) instanceof FireflyBatchVertexReadStepLocal
                    || steps.get(i) instanceof FireflyBatchVertexReadSampleLimitStep
                    || steps.get(i) instanceof FireflyBatchEdgeReadStep
                    || steps.get(i) instanceof FireflyBatchEdgeReadStepLocal
                    || steps.get(i) instanceof FireflyBatchEdgeReadSampleLimitStep
                    || steps.get(i) instanceof FireflyOtherVBatchReadStep
                    || steps.get(i) instanceof FireflyOtherVBatchReadStepLocal
                    || steps.get(i) instanceof FireflyEdgeToVertexBatchReadStep
                    // example: math("b + a").by(in("created").count())
                    || steps.get(i) instanceof MathStep
                    // let's play as safe as possible with repeat step
                    || steps.get(i) instanceof RepeatStep.RepeatEndStep ) {
                return true;
            }
            // recursively test following steps
            if (steps.get(i) instanceof TraversalParent) {
                final List<Traversal.Admin<Object, Object>> children = new ArrayList<>(((TraversalParent) steps.get(i)).getLocalChildren());
                // for steps like match
                children.addAll(((TraversalParent) steps.get(i)).getGlobalChildren());
                for (final Traversal.Admin child : children) {
                    if (areEdgesRequired(child.getSteps(), 0)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }
}
