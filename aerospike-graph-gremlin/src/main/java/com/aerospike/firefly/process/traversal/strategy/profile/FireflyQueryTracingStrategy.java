/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.process.traversal.strategy.profile;

import com.aerospike.firefly.process.computer.util.ComputerHelper;
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
    protected boolean isEnabled(final FireflyGraph fireflyGraph) {
        return fireflyGraph.isQueryTracingEnabled();
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
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
        if (traversal.isRoot()) {
            traversal.addStep(new FireflyQueryTracingStep<>(traversal));
        }
    }
}
