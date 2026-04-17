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
import com.aerospike.firefly.process.traversal.step.map.FireflyVertexEdgeLocalCountStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.ENABLE_EMBEDDED_VERTEX_EDGE_LOCAL_COUNT_STRATEGY;

public class FireflyVertexEdgeLocalCountStrategy extends FireflyStrategyBase {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyVertexEdgeLocalCountStrategy.class);

    /**
     * Default constructor for FireflyVertexEdgeLocalCountStrategy.
     */
    public FireflyVertexEdgeLocalCountStrategy() {
    }

    @Override
    public String getStrategyEnabledKey() {
        return ENABLE_EMBEDDED_VERTEX_EDGE_LOCAL_COUNT_STRATEGY;
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        if (ComputerHelper.onGraphComputer(traversal))
            return;

        for (final LocalStep localStep : TraversalHelper.getStepsOfClass(LocalStep.class, traversal)) {
            final List<Traversal.Admin> localTraversal = localStep.getLocalChildren();
            final List<Step> localTraversalSteps = localTraversal.get(0).getSteps();
            if (localTraversalSteps.size() != 2) {
                continue;
            }
            final VertexStep vertexStep;
            if (!(localTraversalSteps.get(0) instanceof VertexStep)) {
                continue;
            } else {
                vertexStep = (VertexStep) localTraversalSteps.get(0);
                if (vertexStep.getEdgeLabels().length != 0) {
                    continue;
                }
            }
            if (!(localTraversalSteps.get(1) instanceof CountGlobalStep)) {
                continue;
            }

            LOG.debug("Applying FireflyVertexEdgeLocalCountStrategy");
            TraversalHelper.replaceStep(
                    localStep,
                    new FireflyVertexEdgeLocalCountStep(
                            traversal, vertexStep.getDirection(), localStep.getLabels(), ComputerHelper.onGraphComputer(traversal)),
                    traversal);
        }
    }
}
