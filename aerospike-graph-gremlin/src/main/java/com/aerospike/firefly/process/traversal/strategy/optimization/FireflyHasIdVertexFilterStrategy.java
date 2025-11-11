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

import java.util.List;

public class FireflyHasIdVertexFilterStrategy extends FireflyStrategyBase {

    @Override
    public String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_FAST_HASID_VERTEX_FILTER_STRATEGY;
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        // Apply only inside filter parents, skip root and non-filter parents (map/local/etc.) they can violate
        // traversal semantics.
        final Step<?,?> parent = traversal.getParent() == null ? null : traversal.getParent().asStep();
        if (!(parent instanceof org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep
                || parent instanceof org.apache.tinkerpop.gremlin.process.traversal.step.filter.WhereTraversalStep
                || parent instanceof org.apache.tinkerpop.gremlin.process.traversal.step.filter.NotStep)) {
            return;
        }

        final List<Step> steps = traversal.getSteps();
        for (int i = 1; i < steps.size(); i++) {
            final Step<?, ?> current = steps.get(i);
            if (!(current instanceof HasStep)) continue;

            final Step<?, ?> prev = steps.get(i - 1);
            if (!(prev instanceof VertexStep)) continue;

            final VertexStep<?> vertexStep = (VertexStep<?>) prev;
            if (!vertexStep.returnsVertex()) continue;

            final HasStep<?> hasStep = (HasStep<?>) current;
            if (!isIdOnlyFilter(hasStep)) continue;

            // Replace and remove old step
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
    }

    private boolean isIdOnlyFilter(final HasStep<?> hasStep) {
        for (HasContainer hasContainer : hasStep.getHasContainers()) {
            if (!hasContainer.getKey().equals(T.id.getAccessor())) return false;
        }
        return true;
    }
}
