package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.computer.local.ComputerHelper;
import com.aerospike.firefly.process.traversal.step.FireflyCacheGCStep;
import com.aerospike.firefly.process.traversal.step.FireflyDropStep;
import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DropStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.NoneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class FireflyGraphDropStrategy extends FireflyStrategyBase {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyGraphDropStrategy.class);

    /**
     * Default constructor for FireflyGraphDropStrategy.
     */
    public FireflyGraphDropStrategy() {
    }

    @Override
    protected String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_FIREFLY_DROP_STRATEGY;
    }

    /**
     * Check for match of g.V().drop().iterate(), g.V().drop().toList() or g.V().drop().next().
     *
     * @param steps steps.
     * @return true if matches, else false.
     */
    private static boolean matchesToListNextIterate(final List<Step> steps) {
        // Ensure traversal matches g.V().drop().[next|toList]()
        // Note - toList / next are not steps and therefore there is only V().drop() in the steps list.
        if (steps.get(steps.size() -1) instanceof FireflyCacheGCStep) {
            if (steps.size() != 3 && steps.size() != 4) {
                return false;
            }
        } else {
            if (steps.size() != 2 && steps.size() != 3) {
                return false;
            }
        }

        // V()
        final Step vStep = steps.get(0);
        if (!(vStep instanceof GraphStep)) {
            return false;
        } else {
            final GraphStep vGraphStep = (GraphStep) vStep;
            // Ensure step is for vertices: V()
            if (!vGraphStep.returnsVertex()) {
                return false;
            }

            // Ensure step has no ID filter on the vertices: V() should have no parameters
            if (vGraphStep.getIds() != null && vGraphStep.getIds().length != 0) {
                return false;
            }

            // If FireflyGraphStepStrategy has applied to this traversal already, any HasStep will have been removed
            // from the traversal and internalized within the FireflyGraphStep that replaced the original GraphStep.
            // We need to check the FireFlyGraphStep in this case since steps.size() can now equal 3 despite the
            // original traversal containing a HasStep if the FireflyGraphStepStrategy was applied. Summarized, ensure
            // original traversal step V() does not have any HasStep chained after it, e.g. .hasLabel(...)
            if (vGraphStep instanceof FireflyGraphStep) {
                if (((FireflyGraphStep) vGraphStep).getHasContainers().size() != 0) {
                    return false;
                }
            }
        }

        // drop()
        final Step dropStep = steps.get(1);
        if (!(dropStep instanceof DropStep)) {
            return false;
        }

        if ((steps.get(steps.size() -1) instanceof FireflyCacheGCStep && steps.size() == 4)
        || (!(steps.get(steps.size() -1) instanceof FireflyCacheGCStep) && steps.size() == 3)) {
            // iterate()
            final Step iterateStep = steps.get(2);
            if (!(iterateStep instanceof NoneStep)) {
                return false;
            }
        }

        return true;
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        if (!(traversal.isRoot()) || ComputerHelper.onGraphComputer(traversal)) {
            return;
        }
        final List<Step> steps = traversal.getSteps();
        if (matchesToListNextIterate(steps)) {
            LOG.debug("Applying FireflyGraphDropStrategy");
            TraversalHelper.removeAllSteps(traversal);
            traversal.addStep(new FireflyDropStep(traversal));
        }
    }
}
