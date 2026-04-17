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
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Collections;
import java.util.List;
import java.util.Set;

public class FireflyOtherVBatchReadStep extends VertexBatchReadStep {

    public FireflyOtherVBatchReadStep(final Traversal.Admin traversal,
                                      final List<HasContainer> hasContainers,
                                      final Set<String> labels,
                                      final int barrierSize,
                                      final boolean areEdgesRequired,
                                      final long limit) {
        super(traversal, hasContainers, labels, barrierSize, areEdgesRequired, limit);
    }

    @Override
    protected List<FireflyId> getVertexIds(final Traverser.Admin traverser) {
        final List<Object> objects = traverser.path().objects();
        for (int i = objects.size() - 2; i >= 0; i--) {
            if (objects.get(i) instanceof Vertex) {
                final FireflyEdge edge = ((FireflyEdge) traverser.get());
                return List.of(((FireflyVertex) objects.get(i)).id.equals(edge.outVertexId()) ?
                        edge.inVertexId() :
                        edge.outVertexId());
            }
        }

        return List.of();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.PATH);
    }
}
