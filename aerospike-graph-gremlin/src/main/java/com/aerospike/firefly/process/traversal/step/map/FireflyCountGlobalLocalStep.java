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

package com.aerospike.firefly.process.traversal.step.map;

import com.aerospike.firefly.structure.FireflyVertex;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.util.function.ConstantSupplier;

import java.util.List;
import java.util.Set;
import java.util.function.BinaryOperator;

public class FireflyCountGlobalLocalStep<S> extends ReducingBarrierStep<S, Long> {
    private final Direction direction;
    private final String[] edgeLabels;
    private final long limit;
    private final List<HasContainer> hasContainers;

    public FireflyCountGlobalLocalStep(final Traversal.Admin traversal,
                                       final Direction direction,
                                       final Set<String> labels,
                                       final String[] edgeLabels,
                                       final long limit,
                                       final List<HasContainer> hasContainers) {
        super(traversal);
        this.limit = limit;
        this.hasContainers = hasContainers;
        this.setSeedSupplier(new ConstantSupplier<>(0L));
        this.setReducingBiOperator((BinaryOperator) Operator.sumLong);
        this.direction = direction;
        this.labels = labels;
        this.edgeLabels = edgeLabels;
    }

    @Override
    public Long projectTraverser(final Traverser.Admin<S> traverser) {
        final S element = traverser.get();
        final FireflyVertex fireflyVertex;
        if (element instanceof ComputerGraph.ComputerVertex) {
            fireflyVertex = (FireflyVertex) ((ComputerGraph.ComputerVertex) element).getBaseVertex();
        } else {
            fireflyVertex = (FireflyVertex) element;
        }

        final long adjustedLimit = limit == -1 ? -1 : (limit + traverser.bulk() - 1) / traverser.bulk();
        final long result = fireflyVertex.getEdgeCount(direction, edgeLabels, adjustedLimit, hasContainers) * traverser.bulk();
        return limit == -1 || result < limit ? result : limit;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Set.of();
    }
}
