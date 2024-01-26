package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.step.FireflyMergeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyMergeStepStrategy extends FireflyStrategyBase {
    /**
     * Default constructor for FireflyMergeStepStrategy.
     */
    public FireflyMergeStepStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal))
            return;

        for (final MergeVertexStep originalMergeVertexStep : TraversalHelper.getStepsOfClass(MergeVertexStep.class, traversal)) {
            final FireflyMergeVertexStep fireflyMergeVertexStep = new FireflyMergeVertexStep(originalMergeVertexStep);
            TraversalHelper.replaceStep(originalMergeVertexStep, fireflyMergeVertexStep, traversal);
        }

    }
}
