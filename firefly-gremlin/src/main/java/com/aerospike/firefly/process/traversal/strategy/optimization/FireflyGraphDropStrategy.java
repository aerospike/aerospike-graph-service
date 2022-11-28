package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.step.FireflyDropStep;
import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NoneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class FireflyGraphDropStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyGraphDropStrategy.class);
    private static final FireflyGraphDropStrategy INSTANCE = new FireflyGraphDropStrategy();

    private FireflyGraphDropStrategy() {

    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!(traversal.isRoot()) || TraversalHelper.onGraphComputer(traversal))
            return;
        final List<Step> steps = traversal.getSteps();

        // Ensure traversal matches: V().drop().iterate()
        if (steps.size() != 3) {
            return;
        }
        // V()
        final Step vStep = steps.get(0);
        if (!(vStep instanceof GraphStep)) {
            return;
        } else {
            final GraphStep vGraphStep = (GraphStep) vStep;
            // Ensure step is for vertices: V()
            if (!vGraphStep.returnsVertex()) {
                return;
            }
            // Ensure step has no ID filter on the vertices: V() should have no parameters
            if (vGraphStep.getIds().length != 0) {
                return;
            }
            // If FireflyGraphStepStrategy has applied to this traversal already, any HasStep will have been removed
            // from the traversal and internalized within the FireflyGraphStep that replaced the original GraphStep.
            // We need to check the FireFlyGraphStep in this case since steps.size() can now equal 3 despite the
            // original traversal containing a HasStep if the FireflyGraphStepStrategy was applied. Summarized, ensure
            // original traversal step V() does not have any HasStep chained after it, e.g. .hasLabel(...)
            if (vGraphStep instanceof FireflyGraphStep) {
                if (((FireflyGraphStep) vGraphStep).getHasContainers().size() != 0) {
                    return;
                }
            }
        }
        // drop()
        final Step dropStep = steps.get(1);
        if (!(dropStep instanceof DropStep)) {
            return;
        }
        // iterate()
        final Step iterateStep = steps.get(2);
        if (!(iterateStep instanceof NoneStep)) {
            return;
        }

        LOG.debug("Applying FireflyGraphDropStrategy");
        TraversalHelper.removeAllSteps(traversal);
        traversal.addStep(new FireflyDropStep(traversal));
    }

    public static FireflyGraphDropStrategy instance() {
        return INSTANCE;
    }
}
