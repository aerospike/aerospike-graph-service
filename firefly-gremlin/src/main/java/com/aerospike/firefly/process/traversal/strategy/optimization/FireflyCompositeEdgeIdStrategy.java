package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.step.FireflyCompositeIdStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.List;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyCompositeEdgeIdStrategy extends FireflyStrategyBase {

    /**
     * Default constructor for FireflyCompositeEdgeIdStrategy.
     */
    public FireflyCompositeEdgeIdStrategy() {
    }

    @Override
    public String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_COMPOSITE_ID_STRATEGY;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!(traversal.isRoot()) || TraversalHelper.onGraphComputer(traversal))
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

            // Replace vertex step with composite id step.
            traversal.removeStep(vertexStep);
            traversal.addStep(index, new FireflyCompositeIdStep(traversal, vertexStep.getDirection(), vertexStep.getEdgeLabels(), vertexStep.getLabels()));
        }
    }
}
