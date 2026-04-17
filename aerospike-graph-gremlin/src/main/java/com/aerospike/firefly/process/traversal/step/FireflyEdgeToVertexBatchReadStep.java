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

package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.List;
import java.util.Set;

public class FireflyEdgeToVertexBatchReadStep extends VertexBatchReadStep {

    private final Direction direction;

    public FireflyEdgeToVertexBatchReadStep(final Traversal.Admin traversal,
                                            final Direction direction,
                                            final List<HasContainer> hasContainers,
                                            final Set<String> labels,
                                            final int barrierSize,
                                            final boolean areEdgesRequired,
                                            final long limit) {
        super(traversal, hasContainers, labels, barrierSize, areEdgesRequired, limit);
        this.direction = direction;
    }

    @Override
    protected List<FireflyId> getVertexIds(final Traverser.Admin traverser) {
        final FireflyEdge edge = (FireflyEdge) traverser.get();

        switch (direction) {
            case OUT:
                return List.of(edge.outVertexId());
            case IN:
                return List.of(edge.inVertexId());
            default:
                return List.of(edge.outVertexId(), edge.inVertexId());
        }
    }

    public Direction getDirection() {
        return direction;
    }
}
