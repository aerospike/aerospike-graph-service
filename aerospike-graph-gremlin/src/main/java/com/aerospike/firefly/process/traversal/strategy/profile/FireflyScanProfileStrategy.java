package com.aerospike.firefly.process.traversal.strategy.profile;

import com.aerospike.firefly.process.computer.local.ComputerHelper;
import com.aerospike.firefly.process.traversal.step.FireflyScanProfileStep;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyStrategyBase;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Optional;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyScanProfileStrategy extends FireflyStrategyBase {
    public FireflyScanProfileStrategy() {

    }

    @Override
    protected String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_CUSTOM_PROFILE;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {

        if (ComputerHelper.onGraphComputer(traversal))
            return;

        final Optional<Graph> graphOptional = traversal.getGraph();
        if (graphOptional.isEmpty()) {
            return;
        }
        if (!(graphOptional.get() instanceof FireflyGraph)) {
            return;
        }
        if (!(traversal.getStartStep() instanceof GraphStep)) {
            return;
        }

        // Tack on the step that will remove the cache when it's finished.
        final FireflyScanProfileStep profileStep = new FireflyScanProfileStep(traversal);
        // Profile must be last if it exists.
        if (TraversalHelper.hasStepOfClass(ProfileSideEffectStep.class, traversal)) {
            // FireflyProfileStep carries custom metrics
            traversal.addStep(traversal.getSteps().size() - 2, profileStep);
        }
        ((FireflyGraph) graphOptional.get()).getBaseGraph().resetScanHitCounter();
    }
}
