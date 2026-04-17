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

import com.aerospike.firefly.process.traversal.step.util.TraversalUtil;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MapStep;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.NoSuchElementException;
import java.util.Set;

public class FireflyVertexEdgeLocalCountStep extends MapStep<Vertex, Long> {
    private final Direction direction;
    private final boolean isGraphComputer;

    public FireflyVertexEdgeLocalCountStep(final Traversal.Admin traversal, final Direction direction,
                                           final Set<String> labels, final boolean isGraphComputer) {
        super(traversal);
        this.direction = direction;
        this.labels = labels;
        this.isGraphComputer = isGraphComputer;
    }

    @Override
    protected Traverser.Admin<Long> processNextStart() throws NoSuchElementException {
        final Traverser.Admin<Vertex> traverser = this.starts.next();
        final FireflyVertex vertex = isGraphComputer ?
                (FireflyVertex) ((ComputerGraph.ComputerVertex) traverser.get()).getBaseVertex() :
                (FireflyVertex) traverser.get();
        TraversalUtil.supernodeTraversalWarning((FireflyGraph) getTraversal().getGraph().get(), this.traversal, vertex);
        return traverser.split(vertex.getEdgeCount(direction), this);
    }
}
