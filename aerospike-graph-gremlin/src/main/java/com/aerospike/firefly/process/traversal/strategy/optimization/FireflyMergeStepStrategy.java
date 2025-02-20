package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.computer.util.ComputerHelper;
import com.aerospike.firefly.process.traversal.step.FireflyMergeEdgeStep;
import com.aerospike.firefly.process.traversal.step.FireflyMergeVertexStep;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeEdgeStep;
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
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        if (ComputerHelper.onGraphComputer(traversal))
            return;

        for (final MergeVertexStep originalMergeVertexStep : TraversalHelper.getStepsOfClass(MergeVertexStep.class, traversal)) {
            final FireflyMergeVertexStep fireflyMergeVertexStep = new FireflyMergeVertexStep(originalMergeVertexStep);
            TraversalHelper.replaceStep(originalMergeVertexStep, fireflyMergeVertexStep, traversal);
        }

        if (((FireflyGraph) traversal.getGraph().get()).getBaseGraph().isMergeEdgeDataModelEnabled) {
            for (final MergeEdgeStep originalMergeEdgeStep : TraversalHelper.getStepsOfClass(MergeEdgeStep.class, traversal)) {
                final FireflyMergeEdgeStep fireflyMergeEdgeStep = new FireflyMergeEdgeStep(originalMergeEdgeStep);
                TraversalHelper.replaceStep(originalMergeEdgeStep, fireflyMergeEdgeStep, traversal);
            }
        }
    }
}
