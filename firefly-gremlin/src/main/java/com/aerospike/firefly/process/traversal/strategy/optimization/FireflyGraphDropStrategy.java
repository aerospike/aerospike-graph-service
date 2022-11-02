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

import java.util.List;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simon-zhao-69a792ab/</a>)
 */
public class FireflyGraphDropStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {
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
            final FireflyGraphStep vGraphStep = (FireflyGraphStep) vStep;
            // Ensure step is for vertices: V()
            if (!vGraphStep.returnsVertex()) {
                return;
            }
            // Ensure step has no ID filter on the vertices: V() should have no parameters
            if (vGraphStep.getIds().length != 0) {
                return;
            }
            // Ensure step has no filter steps: V() does not have any "has" steps chained after it, e.g. .hasLabel(...)
            if (vGraphStep.getHasContainers().size() != 0) {
                return;
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

        TraversalHelper.removeAllSteps(traversal);
        traversal.addStep(new FireflyDropStep(traversal));
    }

    public static FireflyGraphDropStrategy instance() {
        return INSTANCE;
    }
}
