package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.computer.util.ComputerHelper;
import com.aerospike.firefly.process.traversal.step.map.FireflyCountGlobalLocalStep;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public class FireflyCountGlobalLocalStrategy extends FireflyStrategyBase {
    /**
     * Default constructor for FireflyGraphCountStrategy.
     */
    public FireflyCountGlobalLocalStrategy() {
    }

    @Override
    public String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_FAST_COUNT_STRATEGY;
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        if (!ComputerHelper.onGraphComputer(traversal) || traversal.getSteps().size() < 2)
            return;

        for (int i = 1; i < traversal.getSteps().size(); i++) {
            if (traversal.getSteps().get(i) instanceof CountGlobalStep) {
                if (traversal.getSteps().get(i - 1) instanceof RangeGlobalStep) {
                    final RangeGlobalStep rangeGlobalStep = (RangeGlobalStep) traversal.getSteps().get(i - 1);
                    if (rangeGlobalStep.getLowRange() != 0)
                        continue;
                    if (i > 1 && traversal.getSteps().get(i - 2) instanceof VertexStep) {
                        final VertexStep vertexStep = (VertexStep) traversal.getSteps().get(i - 2);
                        if (vertexStep.getLabels().isEmpty()) {
                            // Need to replace count step and remove vertex and count steps
                            TraversalHelper.replaceStep(
                                    traversal.getSteps().get(i),
                                    new FireflyCountGlobalLocalStep<>(
                                            traversal, vertexStep.getDirection(), traversal.getSteps().get(i).getLabels(),
                                            vertexStep.getEdgeLabels(), rangeGlobalStep.getHighRange()),
                                    traversal);
                            traversal.removeStep(rangeGlobalStep);
                            traversal.removeStep(vertexStep);

                            resetChild(traversal);
                        }
                    }
                } else if (traversal.getSteps().get(i - 1) instanceof VertexStep) {
                    if (i > 1 && traversal.getSteps().get(i - 2) instanceof GraphStep) {
                        final GraphStep graphStep = (GraphStep) traversal.getSteps().get(i - 2);
                        // g.V(<single id>).in/out().count().
                        if (graphStep.getIds().length == 1) {
                            continue;
                        }
                    }

                    final VertexStep vertexStep = (VertexStep) traversal.getSteps().get(i - 1);
                    if (vertexStep.getLabels().isEmpty()) {
                        // Need to replace count step and remove vertex step
                        TraversalHelper.replaceStep(
                                traversal.getSteps().get(i),
                                new FireflyCountGlobalLocalStep<>(
                                        traversal, vertexStep.getDirection(), traversal.getSteps().get(i).getLabels(), vertexStep.getEdgeLabels(), -1),
                                traversal);
                        traversal.removeStep(vertexStep);

                        resetChild(traversal);
                    }
                }
            }
        }
    }

    // workaround for some barrier steps
    private void resetChild(final Traversal.Admin<?, ?> traversal) {
        if (!traversal.isRoot()) {
            try {
                traversal.getParent().replaceLocalChild(traversal, traversal);
            } catch (final IllegalStateException ignored) {
                // some steps don't support child replacement
            }
        }
    }
}
