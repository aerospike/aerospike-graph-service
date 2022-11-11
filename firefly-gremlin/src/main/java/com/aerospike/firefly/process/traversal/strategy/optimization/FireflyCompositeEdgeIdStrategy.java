package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.step.FireflyCompositeIdStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyCompositeEdgeIdStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyCompositeEdgeIdStrategy.class);
    private static final FireflyCompositeEdgeIdStrategy INSTANCE = new FireflyCompositeEdgeIdStrategy();

    private FireflyCompositeEdgeIdStrategy() {

    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!(traversal.isRoot()) || TraversalHelper.onGraphComputer(traversal))
            return;
        final List<Step> steps = traversal.getSteps();

        // We need to find VertexSteps.
        // In particular we need vertex steps that return a vertex.

        for (int index = 0; index < steps.size(); index++) {
            Step<?, ?> step = steps.get(index);
            // If it's not a VertexStep, skip it.
            if (!(step instanceof VertexStep)) {
                continue;
            }

            final VertexStep<?> vertexStep = (VertexStep<?>) step;

            // If it does not return a vertex, skip it. This is the case for something like:
            //  g.V().outE() <- In this case we can let the tinkerpop core handle it.
            if (!vertexStep.returnsVertex()) {
                continue;
            }

            traversal.removeStep(vertexStep);
            traversal.addStep(index, new FireflyCompositeIdStep(traversal, vertexStep.getDirection(), vertexStep.getEdgeLabels()));
            System.out.println("\tVertex step edge labels: " + Arrays.toString(vertexStep.getEdgeLabels()));
            System.out.println("\tVertex step direction: " + vertexStep.getDirection());
            System.out.println("\tVertex step returns vertex: " + vertexStep.returnsVertex());
        }
        // Find in/out steps.
    }

    public static FireflyCompositeEdgeIdStrategy instance() {
        System.out.println("Getting composite id strategy instance.");
        return INSTANCE;
    }
}
