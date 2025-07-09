package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.computer.util.ComputerHelper;
import com.aerospike.firefly.process.traversal.step.map.FireflyAdjacentVertexIdStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;

import java.util.List;
import java.util.Set;

public class FireflyAdjacentVertexIdStrategy extends FireflyStrategyBase {


    public FireflyAdjacentVertexIdStrategy() {
    }

    @Override
    public String getStrategyEnabledKey() {
        // Since we override isEnabled this doesn't really matter.
        return ConfigurationHelper.Keys.ENABLE_CACHED_ADJACENT_ID_STRATEGY;
    }

    @Override
    protected boolean isEnabled(final FireflyGraph graph) {
        return graph.getBaseGraph().ENABLE_CACHED_ADJACENT_ID_STRATEGY;
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        if (ComputerHelper.onGraphComputer(traversal))
            return;

        final List<Step> steps = traversal.getSteps();

        // We need to find VertexSteps.
        // In particular, we need vertex steps that return a vertex.
        for (int index = 0; index < steps.size(); index++) {
            // If it's not a VertexStep, skip it.
            if (!(steps.get(index) instanceof VertexStep)) {
                continue;
            }

            // Cast to VertexStep so we have access to the methods.
            final VertexStep<?> vertexStep = (VertexStep<?>) steps.get(index);

            // If it does not return a vertex, skip it. This is the case for something like:
            //  g.V().outE() <- In this case we can let the tinkerpop core handle it.
            if (!vertexStep.returnsVertex()) {
                continue;
            }
            Set<String> labels = vertexStep.getLabels();

            while (labels.isEmpty()) {
                if (index + 1 >= steps.size()) {
                    break;
                }
                if (steps.get(index + 1) instanceof NoOpBarrierStep) {
                    // Grab any labels and remove the barrier.
                    final NoOpBarrierStep<?> noOpBarrierStep = (NoOpBarrierStep<?>) steps.get(index + 1);
                    labels = noOpBarrierStep.getLabels();

                    // If there are labels we cannot place our step in so break.
                    if (!labels.isEmpty()) {
                        break;
                    }

                    // No labels in barrier so we can remove it without impact.
                    traversal.removeStep(steps.get(index + 1));
                } else if (steps.get(index + 1) instanceof IdStep) {
                    // Grab any labels and remove the identity step.
                    final IdStep<?> idStep = (IdStep<?>) steps.get(index + 1);
                    labels = idStep.getLabels();
                    traversal.removeStep(idStep);
                    traversal.addStep(index + 1,
                            new FireflyAdjacentVertexIdStep(traversal.asAdmin(),
                                    labels,
                                    vertexStep.getDirection(),
                                    vertexStep.getEdgeLabels()));
                    traversal.removeStep(vertexStep);
                    break;
                } else {
                    break;
                }
            }
        }
    }
}
