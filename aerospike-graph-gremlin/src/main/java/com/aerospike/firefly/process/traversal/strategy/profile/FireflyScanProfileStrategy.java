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
import com.aerospike.firefly.process.traversal.step.FireflyScanProfileStep;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyStrategyBase;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Optional;

public class FireflyScanProfileStrategy extends FireflyStrategyBase {

    public FireflyScanProfileStrategy() {
    }

    @Override
    protected String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_CUSTOM_PROFILE;
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
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
