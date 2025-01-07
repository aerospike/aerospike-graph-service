package com.aerospike.firefly.process.traversal.strategy.profile;

import com.aerospike.firefly.process.computer.local.ComputerHelper;
import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyQueryTracingStep;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyStrategyBase;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ProfilingAware;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.List;

public class FireflyQueryTracingStrategy extends FireflyStrategyBase {

    @Override
    public boolean isEnabled(final FireflyGraph fireflyGraph) {
        return fireflyGraph.isQueryTracingEnabled();
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (ComputerHelper.onGraphComputer(traversal) ||
                TraversalHelper.hasStepOfClass(ProfileSideEffectStep.class, traversal)) {
            return;
        }

        // Add .profile() step after every pre-existing step.
        final List<Step> steps = traversal.getSteps();
        final int numSteps = steps.size();
        for (int i = 0; i < numSteps; i++) {
            // Create and inject ProfileStep
            final ProfileStep profileStepToAdd = new ProfileStep(traversal);
            traversal.addStep((i * 2) + 1, profileStepToAdd);

            final Step stepToBeProfiled = traversal.getSteps().get(i * 2);
            if (stepToBeProfiled instanceof ProfilingAware) {
                ((ProfilingAware) stepToBeProfiled).prepareForProfiling();
            }
        }
        traversal.addStep(new FireflyQueryTracingStep<>(traversal));
    }
}
