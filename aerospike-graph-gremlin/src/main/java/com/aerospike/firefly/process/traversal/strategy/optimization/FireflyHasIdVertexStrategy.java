package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.step.FireflyHasIdVertexStep;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.List;

public class FireflyHasIdVertexStrategy extends FireflyStrategyBase {

    @Override
    public String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_FAST_HASID_VERTEX_STRATEGY;
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        final List<Step> steps = traversal.getSteps();
        if (steps.size() < 2) return;

        for (int i = 1; i < steps.size(); i++) {
            final Step<?, ?> currentStep = steps.get(i);

            if (currentStep instanceof HasStep && steps.get(i - 1) instanceof VertexStep) {
                final HasStep<?> hasStep = (HasStep<?>) currentStep;
                final VertexStep<?> vertexStep = (VertexStep<?>) steps.get(i - 1);

                if (!vertexStep.returnsVertex()) continue;
                if (!isIdOnlyFilter(hasStep)) continue;

                TraversalHelper.replaceStep(
                        hasStep,
                        new FireflyHasIdVertexStep<>(
                                traversal,
                                vertexStep.getDirection(),
                                vertexStep.getEdgeLabels(),
                                hasStep.getHasContainers()
                        ),
                        traversal
                );

                traversal.removeStep(vertexStep);
            }
        }
    }

    private boolean isIdOnlyFilter(final HasStep<?> hasStep) {
        for (HasContainer hasContainer : hasStep.getHasContainers()) {
            if (!hasContainer.getKey().equals(T.id.getAccessor())) return false;
        }
        return true;
    }
}
