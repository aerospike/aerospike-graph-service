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

import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public class FireflyHasIdVertexFilterStep<S> extends AbstractStep<S, S> {
    private final Direction direction;
    private final Set<String> edgeLabels;
    private final List<HasContainer> hasContainers;

    public FireflyHasIdVertexFilterStep(final Traversal.Admin<?, ?> traversal,
                                        final Direction direction,
                                        final String[] edgeLabels,
                                        final List<HasContainer> hasContainers) {
        super(traversal);
        this.direction = direction;
        this.edgeLabels = new HashSet<>(Arrays.asList(edgeLabels));
        this.hasContainers = hasContainers;
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        while (true) {
            final Traverser.Admin<S> traverser = this.starts.next();
            final S element = traverser.get();

            final FireflyVertex vertex;
            if (element instanceof FireflyVertex) {
                vertex = (FireflyVertex) element;
            } else if (element instanceof ComputerGraph.ComputerVertex) {
                vertex = (FireflyVertex) ((ComputerGraph.ComputerVertex) element).getBaseVertex();
            } else {
                continue; // Skip non-vertex traversers
            }

            final Iterator<FireflyId> neighborIds =
                    vertex.getVertexIdsFromVertex(direction, edgeLabels);

            while (neighborIds.hasNext()) {
                final FireflyId neighborId = neighborIds.next();
                final ReferenceVertex ref = new ReferenceVertex(neighborId.getUserId());

                boolean allMatch = true;
                for (final HasContainer hc : hasContainers) {
                    if (!hc.test(ref)) {
                        allMatch = false;
                        break;
                    }
                }
                if (allMatch) {
                    return traverser;
                }
            }
            // Otherwise, skip this traverser
        }
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
