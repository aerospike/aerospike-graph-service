package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.computer.util.ComputerHelper;
import com.aerospike.firefly.process.traversal.step.map.FireflyCountGlobalLocalStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public class FireflyCountGlobalLocalStrategy  extends FireflyStrategyBase {
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
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!traversal.isRoot()) {
            final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();
            if (!graph.getBaseGraph().ENABLE_EMBEDDED_GRAPH_COUNT_STRATEGY) {
                return;
            }
        }

        if (!ComputerHelper.onGraphComputer(traversal))
            return;
        for (int i = 0; i < traversal.getSteps().size(); i++) {
            if (traversal.getSteps().get(i) instanceof CountGlobalStep) {
                if (i > 0 && traversal.getSteps().get(i - 1) instanceof VertexStep) {
                    final VertexStep vertexStep = (VertexStep) traversal.getSteps().get(i - 1);
                    if (vertexStep.getLabels().isEmpty()) {
                        // Need to replace count step and remove vertex step
                        TraversalHelper.replaceStep(
                                traversal.getSteps().get(i),
                                new FireflyCountGlobalLocalStep<>(
                                        traversal, vertexStep.getDirection(), traversal.getSteps().get(i).getLabels()),
                                traversal);
                        traversal.removeStep(vertexStep);
                    }

                }
            }
    }
    }
}
