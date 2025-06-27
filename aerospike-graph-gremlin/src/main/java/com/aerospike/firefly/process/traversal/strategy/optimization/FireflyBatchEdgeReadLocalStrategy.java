package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.computer.util.ComputerHelper;
import com.aerospike.firefly.process.traversal.step.computer.FireflyBatchEdgeReadStepLocal;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.List;
import java.util.Set;

import static com.aerospike.firefly.process.traversal.strategy.util.StrategyHelper.areEdgesRequired;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyBatchEdgeReadLocalStrategy extends FireflyStrategyBase {

    /**
     * Default constructor for FireflyBatchEdgeReadStrategy.
     */
    public FireflyBatchEdgeReadLocalStrategy() {
    }

    @Override
    public String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_BATCH_EDGE_READ_STRATEGY;
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();

        if (!ComputerHelper.onGraphComputer(traversal))
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

            // If it does return a vertex, skip it. This is the case for something like:
            //  g.V().out() <- In this case we can let the tinkerpop core handle it.
            if (vertexStep.returnsVertex()) {
                continue;
            }

            // Replace vertex step with composite id step.
            traversal.removeStep(vertexStep);

            // Peek after the VertexStep to see if there is any HasStep's. If there are grab them all and push
            // them into the FireflyVertexStep.
            // Null has container is okay, it simply means that there is no HasContainer.
            // Note we don't want to push down ids.
            List<HasContainer> hasContainers = null;
            Set<String> labels = vertexStep.getLabels();
            final boolean areEdgesRequired = areEdgesRequired(traversal, steps, index);
            while (labels.isEmpty()) {
                if (index >= steps.size()) {
                    break;
                }
                if (steps.get(index) instanceof NoOpBarrierStep) {
                    // Grab any labels and remove the barrier.
                    final NoOpBarrierStep<?> noOpBarrierStep = (NoOpBarrierStep<?>) steps.get(index);
                    labels = noOpBarrierStep.getLabels();
                    traversal.removeStep(steps.get(index));
                } else if (steps.get(index) instanceof HasStep) {
                    hasContainers = ((HasStep) steps.get(index)).getHasContainers();
                    labels = steps.get(index).getLabels();
                    traversal.removeStep(steps.get(index));
                } else {
                    // Unknown step, break.
                    break;
                }
            }
            traversal.addStep(index, new FireflyBatchEdgeReadStepLocal(
                    traversal,
                    vertexStep.getDirection(),
                    vertexStep.getEdgeLabels(),
                    labels,
                    hasContainers,
                    areEdgesRequired));

        }
    }
}
