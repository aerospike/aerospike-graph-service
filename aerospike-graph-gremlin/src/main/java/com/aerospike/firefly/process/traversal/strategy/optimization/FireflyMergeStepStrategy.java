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
import com.aerospike.firefly.process.traversal.step.FireflyMergeEdgeStep;
import com.aerospike.firefly.process.traversal.step.FireflyMergeVertexStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.aerospike.firefly.util.exceptions.GraphError;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeEdgeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MergeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

public class FireflyMergeStepStrategy extends FireflyStrategyBase {
    /**
     * Default constructor for FireflyMergeStepStrategy.
     */
    public FireflyMergeStepStrategy() {
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        if (ComputerHelper.onGraphComputer(traversal))
            return;

        for (final MergeVertexStep originalMergeVertexStep : TraversalHelper.getStepsOfClass(MergeVertexStep.class, traversal)) {
            final FireflyMergeVertexStep fireflyMergeVertexStep = new FireflyMergeVertexStep(originalMergeVertexStep);
            TraversalHelper.replaceStep(originalMergeVertexStep, fireflyMergeVertexStep, traversal);
        }

        for (final MergeEdgeStep originalMergeEdgeStep : TraversalHelper.getStepsOfClass(MergeEdgeStep.class, traversal)) {
            // If we cannot run mergeE queries (expiration disabled), we should error early.
            final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();
            if (!graph.getBaseGraph().getConfig().expirationEnabled) {
                throw new AerospikeGraphException(GraphError.NSUP_DISABLED);
            }
            final FireflyMergeEdgeStep fireflyMergeEdgeStep = new FireflyMergeEdgeStep(originalMergeEdgeStep);
            TraversalHelper.replaceStep(originalMergeEdgeStep, fireflyMergeEdgeStep, traversal);
        }
    }
}
