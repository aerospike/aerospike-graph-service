package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.step.FireflyHasIdVertexFilterStep;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;

public class FireflyHasIdVertexFilterStrategy extends FireflyStrategyBase {

    @Override
    public String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_FAST_HASID_VERTEX_FILTER_STRATEGY;
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        // Only optimize the simple 2-step pattern: VertexStep → HasStep
        if (traversal.getSteps().size() != 2) {
            return;
        }

        // Only allowed under filter parents
        final Step<?, ?> parent = traversal.getParent() == null ? null : traversal.getParent().asStep();
        if (!(parent instanceof org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep
                || parent instanceof org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep
                || parent instanceof org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep)) {
            return;
        }

        // Extract the only two steps
        final Step<?, ?> prev = traversal.getSteps().get(0);
        final Step<?, ?> current = traversal.getSteps().get(1);

        // Pattern must be VertexStep → HasStep
        if (!(prev instanceof VertexStep)) return;
        if (!(current instanceof HasStep)) return;

        final VertexStep<?> vertexStep = (VertexStep<?>) prev;
        if (!vertexStep.returnsVertex()) return;

        final HasStep<?> hasStep = (HasStep<?>) current;
        if (!isIdOnlyFilter(hasStep)) return;

        // Replace with optimized step and remove old step
        TraversalHelper.replaceStep(
                hasStep,
                new FireflyHasIdVertexFilterStep<>(
                        traversal,
                        vertexStep.getDirection(),
                        vertexStep.getEdgeLabels(),
                        hasStep.getHasContainers()
                ),
                traversal
        );
        traversal.removeStep(vertexStep);
    }

    private boolean isIdOnlyFilter(final HasStep<?> hasStep) {
        for (HasContainer hasContainer : hasStep.getHasContainers()) {
            if (!hasContainer.getKey().equals(T.id.getAccessor())) return false;
        }
        return true;
    }
}
