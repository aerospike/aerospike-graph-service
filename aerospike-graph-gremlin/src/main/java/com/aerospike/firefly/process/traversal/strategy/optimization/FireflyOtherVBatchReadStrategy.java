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

package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.computer.util.ComputerHelper;
import com.aerospike.firefly.process.traversal.step.FireflyOtherVBatchReadStep;
import com.aerospike.firefly.process.traversal.step.computer.FireflyOtherVBatchReadStepLocal;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.List;
import java.util.Set;

import static com.aerospike.firefly.process.traversal.strategy.util.StrategyHelper.areEdgesRequired;

public class FireflyOtherVBatchReadStrategy extends FireflyStrategyBase {

    public FireflyOtherVBatchReadStrategy() {
    }

    @Override
    public String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_BATCH_VERTEX_READ_OTHERV_STRATEGY;
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();

        if (!graph.getBaseGraph().getConfig().enableBatchVertexReadOtherVStrategy) {
            return;
        }

        final List<Step> steps = traversal.getSteps();

        for (int index = 0; index < steps.size(); index++) {
            if (!(steps.get(index) instanceof EdgeOtherVertexStep)) {
                continue;
            }

            final Step original = steps.get(index);

            List<HasContainer> hasContainers = null;
            Set<String> labels = original.getLabels();

            final boolean areEdgesRequired = areEdgesRequired(traversal, steps, index);
            long limitSize = -1;

            // there is one more following step, may be filter?
            while (labels.isEmpty()) {
                if (index + 1 >= steps.size()) {
                    break;
                }
                if (steps.get(index + 1) instanceof NoOpBarrierStep) {
                    // Grab any labels and remove the barrier.
                    final NoOpBarrierStep<?> noOpBarrierStep = (NoOpBarrierStep<?>) steps.get(index + 1);
                    labels = noOpBarrierStep.getLabels();
                    traversal.removeStep(steps.get(index + 1));
                } else if (steps.get(index + 1) instanceof HasStep) {
                    // Grab has containers and push them down.
                    final HasStep<?> hasStep = (HasStep<?>) steps.get(index + 1);
                    hasContainers = hasStep.getHasContainers();
                    labels = hasStep.getLabels();
                    traversal.removeStep(steps.get(index + 1));
                } else if (steps.get(index + 1) instanceof RangeGlobalStep) {
                    if (!graph.getBaseGraph().getConfig().enableBatchEdgeReadLimitStrategy) {
                        break;
                    }
                    final long low = ((RangeGlobalStep<?>) steps.get(index + 1)).getLowRange();
                    final long high = ((RangeGlobalStep<?>) steps.get(index + 1)).getHighRange();

                    if (low != 0) {
                        break;
                    }

                    // Get the limit size.
                    limitSize = high;
                    break; // if there's has containers after limit we shouldn't push it down.
                } else {
                    // Unknown step, break.
                    break;
                }
            }

            final Step<?, ?> optimizedStep;
            if (ComputerHelper.onGraphComputer(traversal)) {
                optimizedStep = new FireflyOtherVBatchReadStepLocal(
                        traversal,
                        hasContainers,
                        labels,
                        areEdgesRequired);
            } else {
                optimizedStep = new FireflyOtherVBatchReadStep(
                        traversal,
                        hasContainers,
                        labels,
                        graph.getBaseGraph().getConfig().movementBarrierSize,
                        areEdgesRequired,
                        limitSize);
            }

            TraversalHelper.replaceStep(original, optimizedStep, traversal);
        }
    }
}
