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
import com.aerospike.firefly.process.traversal.step.FireflyBatchEdgeReadStep;
import com.aerospike.firefly.process.traversal.step.FireflyBatchEdgeReadSampleLimitStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Set;

public class FireflyBatchEdgeReadStrategy extends FireflyStrategyBase {

    /**
     * Default constructor for FireflyBatchEdgeReadStrategy.
     */
    public FireflyBatchEdgeReadStrategy() {
    }

    @Override
    public String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_BATCH_EDGE_READ_STRATEGY;
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();

        if (ComputerHelper.onGraphComputer(traversal))
            return;

        final List<Step> steps = traversal.getSteps();

        // We need to find VertexSteps.
        // In particular, we need vertex steps that return a vertex.
        for (int index = 0; index < steps.size(); index++) {
            // If it's not a VertexStep, skip it.
            if (!(steps.get(index) instanceof VertexStep)) {
                continue;
            }

            // Cast to VertexStep so we have access to the methods.
            final VertexStep<?> vertexStep = (VertexStep<?>) steps.get(index);

            // If it does not return a vertex, skip it. This is the case for something like:
            //  g.V().outE() <- In this case we can let the tinkerpop core handle it.
            if (!vertexStep.returnsEdge()) {
                continue;
            }
            traversal.removeStep(vertexStep);

            // Peek after the VertexStep to see if there is any HasStep's. If there are grab them all and push
            // them into the FireflyVertexStep.
            // Null has container is okay, it simply means that there is no HasContainer.
            // Note we don't want to push down ids.
            List<HasContainer> hasContainers = null;
            Set<String> labels = vertexStep.getLabels();
            List<HasContainer> adjustedIdContainers = null;

            int sampleSize = -1;
            long limitSize = -1;

            while (labels.isEmpty()) {
                if (index >= steps.size()) {
                    break;
                }
                if (steps.get(index) instanceof NoOpBarrierStep) {
                    // Grab any labels and remove the barrier.
                    final NoOpBarrierStep<?> noOpBarrierStep = (NoOpBarrierStep<?>) steps.get(index);
                    labels = noOpBarrierStep.getLabels();
                    traversal.removeStep(steps.get(index));
                } else if (steps.get(index) instanceof HasStep) {
                    if (sampleSize != -1 || limitSize != -1) {
                        // outE().limit/sample(<amount>).has(...)
                        // Can't pushdown HasContainers, therefore just break here and let them be applied after.
                        break;
                    }
                    hasContainers = ((HasStep) steps.get(index)).getHasContainers();
                    labels = steps.get(index).getLabels();
                    traversal.removeStep(steps.get(index));
                } else if (steps.get(index) instanceof TraversalFilterStep) {
                    final Traversal.Admin filterTraversal = ((TraversalFilterStep) steps.get(index)).getFilterTraversal();
                    final List<Step> filterSteps = filterTraversal.getSteps();

                    // Check for labels that need to be propagated.
                    if (!((TraversalFilterStep<?>) steps.get(index)).getLabels().isEmpty()) {
                        labels = ((TraversalFilterStep<?>) steps.get(index)).getLabels();
                    }

                    // we support only __.otherV().hasId(<id2) pattern
                    // following if's can be combined into one, but good luck reading that.
                    if (filterSteps.size() != 2 || !(filterSteps.get(1) instanceof HasStep)) {
                        break;
                    }
                    // bothE().where(__.otherV().hasId(<id2))
                    if (vertexStep.getDirection() == Direction.BOTH && !(filterSteps.get(0) instanceof EdgeOtherVertexStep)) {
                        break;
                    }
                    // inE().where(__.outV().hasId(<id2))
                    if (vertexStep.getDirection() == Direction.IN
                            && (!(filterSteps.get(0) instanceof EdgeVertexStep)
                                || ((EdgeVertexStep) filterSteps.get(0)).getDirection() == Direction.IN)) {
                        break;
                    }
                    // inE().where(__.outV().hasId(<id2))
                    if (vertexStep.getDirection() == Direction.OUT
                            && (!(filterSteps.get(0) instanceof EdgeVertexStep)
                                || ((EdgeVertexStep) filterSteps.get(0)).getDirection() == Direction.OUT)) {
                        break;
                    }

                    final List<HasContainer> containers = ((HasStep) filterSteps.get(1)).getHasContainers();
                    // only predicates by t.id supported
                    if (containers.stream().allMatch(c -> !c.getKey().equals(T.id.getAccessor()))) {
                        break;
                    }

                    // remove filter step
                    traversal.removeStep(steps.get(index));
                    adjustedIdContainers = containers;
                } else if (steps.get(index) instanceof SampleGlobalStep) {
                    if (!graph.getBaseGraph().getConfig().enableBatchEdgeReadSamplingStrategy) {
                        break;
                    }
                    try {
                        // The sample size is private, need to use reflection to get it so the compiler doesn't complain.
                        final Field sampleField = SampleGlobalStep.class.getDeclaredField("amountToSample");
                        sampleField.setAccessible(true);

                        // Get the sample size.
                        sampleSize = (int) sampleField.get(steps.get(index));

                        // Check for labels that need to be propagated.
                        if (!((SampleGlobalStep<?>) steps.get(index)).getLabels().isEmpty()) {
                            labels = ((SampleGlobalStep<?>) steps.get(index)).getLabels();
                        }

                        // Add limit step, otherwise each instance of the composite id step will sample the
                        // sample amount.
                        //
                        // This means if you sampled 30 and have 1000 items in and a barrier size of 100
                        // you would output 300 items, not 30, the limit breaks the barrier before they all run.
                        //
                        // This does bring the randomness of our step into question, however since our input
                        // is inherently unordered and out of our control, it seems sufficiently random
                        // for our purposes. Also, this can be disabled if truer randomness is needed.
                        traversal.removeStep(steps.get(index));
                        final RangeGlobalStep<?> step = new RangeGlobalStep<>(traversal.asAdmin(), 0, sampleSize);

                        // Labels should be propagated after the limit step.
                        if (!labels.isEmpty()) {
                            for (final String label : labels) {
                                step.addLabel(label);
                            }
                            labels.clear();
                        }
                        traversal.addStep(index, step);
                        break;
                    } catch (final NoSuchFieldException | IllegalAccessException ignored) {
                        // Failed to get sample size, just ignore it.
                    }
                } else if (steps.get(index) instanceof RangeGlobalStep) {
                    if (sampleSize != -1 || limitSize != -1) {
                        // Already grabbed this, still looping to see if a HasStep is present, but we found this instead.
                        break;
                    }
                    if (!graph.getBaseGraph().getConfig().enableBatchEdgeReadLimitStrategy) {
                        break;
                    }

                    final long low = ((RangeGlobalStep<?>) steps.get(index)).getLowRange();
                    final long high = ((RangeGlobalStep<?>) steps.get(index)).getHighRange();

                    if (low != 0) {
                        break;
                    }

                    // label propagation is not required for limit step b/c it is not removed.

                    // Get the limit size.
                    limitSize = high;

                    // No need to add limit set since it's already there.
                    // Cannot push down HasStep after pushing down sample so need to break.
                    break;
                } else {
                    // Unknown step, break.
                    break;
                }
            }

            boolean hasPushdown = hasContainers != null && !hasContainers.isEmpty() || adjustedIdContainers != null && !adjustedIdContainers.isEmpty();
            if ((limitSize != -1 || sampleSize != -1) && !hasPushdown) {
                traversal.addStep(index, new FireflyBatchEdgeReadSampleLimitStep(
                        traversal,
                        vertexStep.getDirection(),
                        vertexStep.getEdgeLabels(),
                        labels,
                        sampleSize,
                        limitSize,
                        graph.getBaseGraph().getConfig().movementBarrierSize));
            } else {
                traversal.addStep(index, new FireflyBatchEdgeReadStep(
                        traversal,
                        vertexStep.getDirection(),
                        vertexStep.getEdgeLabels(),
                        labels,
                        hasContainers,
                        adjustedIdContainers,
                        graph.getBaseGraph().getConfig().movementBarrierSize,
                        limitSize));
                if (sampleSize != -1) {
                    // If we have a sample size, we need to add a limit step after the batch edge read step.
                    final SampleGlobalStep<?> step = new SampleGlobalStep<>(traversal.asAdmin(), sampleSize);
                    traversal.addStep(index + 1, step);
                }
            }
        }
    }
}
