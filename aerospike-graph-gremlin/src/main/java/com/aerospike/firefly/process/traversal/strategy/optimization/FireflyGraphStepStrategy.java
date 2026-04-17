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

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.List;

import static com.aerospike.firefly.process.traversal.strategy.util.StrategyHelper.getPropertyKeys;
import static com.aerospike.firefly.process.traversal.strategy.util.StrategyHelper.isPropertyRemovalValid;
import static com.aerospike.firefly.process.traversal.strategy.util.StrategyHelper.isPropertyStep;
import static com.aerospike.firefly.process.traversal.strategy.util.StrategyHelper.isVertexOrEdgeStep;

public class FireflyGraphStepStrategy extends FireflyStrategyBase {

    /**
     * Default constructor for FireflyGraphStepStrategy.
     */
    public FireflyGraphStepStrategy() {
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        final boolean propertyRemovalValid = isPropertyRemovalValid(traversal);

        for (final GraphStep originalGraphStep : TraversalHelper.getStepsOfClass(GraphStep.class, traversal)) {
            int labelCount = 0;
            labelCount += originalGraphStep.getLabels().size();
            final FireflyGraphStep<?, ?> fireflyGraphStep = new FireflyGraphStep<>(originalGraphStep);
            TraversalHelper.replaceStep(originalGraphStep, fireflyGraphStep, traversal);
            Step<?, ?> currentStep = fireflyGraphStep.getNextStep();
            while (currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep) {
                if (currentStep instanceof HasStep) {
                    for (final HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
                        if (!GraphStep.processHasContainerIds(fireflyGraphStep, hasContainer))
                            fireflyGraphStep.addHasContainer(hasContainer);
                    }
                    TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
                    traversal.removeStep(currentStep);
                }
                currentStep = currentStep.getNextStep();
            }
            if (propertyRemovalValid && labelCount == 0) {
                if (currentStep instanceof VertexStep || currentStep instanceof IdStep) {
                    final List<String> properties = getPropertyKeys(fireflyGraphStep.getHasContainers());
                    fireflyGraphStep.addProperties(properties);
                } else if (isPropertyStep(currentStep)) {
                    final List<String> propertyKeys = getPropertyKeys(currentStep);
                    if (!propertyKeys.isEmpty()) {
                        final List<String> properties = getPropertyKeys(fireflyGraphStep.getHasContainers());
                        for (final String propertyKey : propertyKeys) {
                            if (!properties.contains(propertyKey)) {
                                properties.add(propertyKey);
                            }
                        }
                        fireflyGraphStep.addProperties(properties);
                    }
                } else if (isVertexOrEdgeStep(currentStep)) {
                    fireflyGraphStep.addProperties(getPropertyKeys(fireflyGraphStep.getHasContainers()));
                }
            }
        }
    }
}
