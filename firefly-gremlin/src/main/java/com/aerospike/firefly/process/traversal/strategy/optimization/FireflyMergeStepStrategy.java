package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.step.FireflyMergeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyMergeStepStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    private static final FireflyMergeStepStrategy INSTANCE = new FireflyMergeStepStrategy();

    private FireflyMergeStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal))
            return;

        for (final MergeVertexStep originalMergeVertexStep : TraversalHelper.getStepsOfClass(MergeVertexStep.class, traversal)) {
            final FireflyMergeVertexStep tinkerMergeVertexStep = new FireflyMergeVertexStep(originalMergeVertexStep);
            TraversalHelper.replaceStep(originalMergeVertexStep, tinkerMergeVertexStep, traversal);
        }

    }

    public static FireflyMergeStepStrategy instance() {
        return INSTANCE;
    }
}