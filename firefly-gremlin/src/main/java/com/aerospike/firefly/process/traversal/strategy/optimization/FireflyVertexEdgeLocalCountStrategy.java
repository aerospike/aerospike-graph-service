package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.step.map.FireflyVertexEdgeLocalCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class FireflyVertexEdgeLocalCountStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyVertexEdgeLocalCountStrategy.class);
    private static final FireflyVertexEdgeLocalCountStrategy INSTANCE = new FireflyVertexEdgeLocalCountStrategy();

    private FireflyVertexEdgeLocalCountStrategy() {

    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!(traversal.isRoot()) || TraversalHelper.onGraphComputer(traversal))
            return;

        for (final LocalStep localStep : TraversalHelper.getStepsOfClass(LocalStep.class, traversal)) {
            final List<Traversal.Admin> localTraversal = localStep.getLocalChildren();
            final List<Step> localTraversalSteps = localTraversal.get(0).getSteps();
            if (localTraversalSteps.size() != 2) {
                continue;
            }
            final VertexStep vertexStep;
            if (!(localTraversalSteps.get(0) instanceof VertexStep)) {
                continue;
            } else {
                vertexStep = (VertexStep) localTraversalSteps.get(0);
                if (vertexStep.getEdgeLabels().length != 0) {
                    continue;
                }
            }
            if (!(localTraversalSteps.get(1) instanceof CountGlobalStep)) {
                continue;
            }

            LOG.debug("Applying FireflyVertexEdgeLocalCountStrategy");
            TraversalHelper.replaceStep(
                    localStep,
                    new FireflyVertexEdgeLocalCountStep(traversal, vertexStep.getDirection(), localStep.getLabels()),
                    traversal);
        }
    }

    public static FireflyVertexEdgeLocalCountStrategy instance() {
        return INSTANCE;
    }
}
