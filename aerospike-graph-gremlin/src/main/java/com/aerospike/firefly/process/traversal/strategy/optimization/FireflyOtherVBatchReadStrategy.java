package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.computer.util.ComputerHelper;
import com.aerospike.firefly.process.traversal.step.FireflyOtherVBatchReadStep;
import com.aerospike.firefly.process.traversal.step.computer.FireflyOtherVBatchReadStepLocal;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static com.aerospike.firefly.process.traversal.strategy.util.StrategyHelper.requiresEdges;

public class FireflyOtherVBatchReadStrategy extends FireflyStrategyBase {

    public FireflyOtherVBatchReadStrategy() {
    }

    @Override
    public String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_BATCH_VERTEX_READ_OTHERV_STRATEGY;
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();

        if (!graph.getBaseGraph().ENABLE_BATCH_VERTEX_READ_OTHERV_STRATEGY) {
            return;
        }

        final List<Step> steps = traversal.getSteps();

        for (int index = 0; index < steps.size(); index++) {
            if (!(steps.get(index) instanceof EdgeOtherVertexStep)) {
                continue;
            }

            final Step original = steps.get(index);

            List<HasContainer> hasContainers = null;
            Set<String> labels = original.getLabels();

            final boolean requiresEdges = requiresEdges(traversal, steps, index);

            // there is one more following step, may be filter?
            while (labels.isEmpty()) {
                if (index + 1 >= steps.size()) {
                    break;
                }
                if (steps.get(index + 1) instanceof NoOpBarrierStep) {
                    // Grab any labels and remove the barrier.
                    final NoOpBarrierStep<?> noOpBarrierStep = (NoOpBarrierStep<?>) steps.get(index + 1);
                    labels = noOpBarrierStep.getLabels();
                    traversal.removeStep(steps.get(index + 1));
                } else if (steps.get(index + 1) instanceof HasStep) {
                    // Grab has containers and push them down.
                    final HasStep<?> hasStep = (HasStep<?>) steps.get(index + 1);
                    hasContainers = hasStep.getHasContainers();

                    // No support for pushdown of primary key check at this time.
                    // This isn't really a useful pushdown anyway.
                    if (hasContainers.stream().map(HasContainer::getKey).noneMatch(key -> key.equals(T.id.getAccessor()))) {
                        labels = hasStep.getLabels();
                        traversal.removeStep(hasStep);

                        // Cannot use sample strategy after HasStep at this time so break.
                        break;
                    } else {
                        hasContainers = new ArrayList<>();
                        break;
                    }
                } else {
                    // Unknown step, break.
                    break;
                }
            }

            final Step<?, ?> optimizedStep;
            if (ComputerHelper.onGraphComputer(traversal)) {
                optimizedStep = new FireflyOtherVBatchReadStepLocal(
                        traversal,
                        hasContainers,
                        labels,
                        requiresEdges);
            } else {
                optimizedStep = new FireflyOtherVBatchReadStep(
                        traversal,
                        hasContainers,
                        labels,
                        graph.getBaseGraph().MOVEMENT_BARRIER_SIZE,
                        requiresEdges);
            }

            TraversalHelper.replaceStep(original, optimizedStep, traversal);
        }
    }
}
