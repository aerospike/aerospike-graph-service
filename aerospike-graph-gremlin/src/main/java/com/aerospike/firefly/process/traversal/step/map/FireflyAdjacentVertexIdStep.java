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

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyIdComposite;
import com.aerospike.firefly.structure.iterator.FireflyBatchElementIterator;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FlatMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

public class FireflyAdjacentVertexIdStep extends FlatMapStep<Vertex, Object> implements AutoCloseable {
    private final String[] edgeLabels;
    private Direction direction;

    public FireflyAdjacentVertexIdStep(final Traversal.Admin traversal,
                                       final Set<String> labels,
                                       final Direction direction,
                                       final String... edgeLabels) {
        super(traversal);
        this.labels = labels;
        this.direction = direction;
        this.edgeLabels = edgeLabels;
    }

    @Override
    protected Iterator<Object> flatMap(final Traverser.Admin<Vertex> traverser) {
        final FireflyVertex vertex = (FireflyVertex) traverser.get();
        final Iterator<FireflyId> cachedCompositeIds = vertex.getCachedIds(this.direction, Set.of(edgeLabels)).iterator();
        Iterator<Object> userIds = IteratorUtils.map(cachedCompositeIds, fireflyId -> {
            final FireflyIdComposite compositeId = (FireflyIdComposite) fireflyId;
            return compositeId.getAdjacentUserId();
        });
        if (vertex.isEdgeCacheOverflowed()) {
            final FireflyGraph graph = (FireflyGraph) this.traversal.getGraph().get();
            final Iterator<FireflyId> supernodeVertexIds = vertex.getSupernodeVertexIds(direction, Set.of(edgeLabels));
            final Iterator<Vertex> adjacentVertices = new FireflyBatchElementIterator<>(graph, supernodeVertexIds,
                    Collections.emptyList(), graph::readVertices, Collections.emptyList());
            final Iterator<Object> adjacentVerticesIds = IteratorUtils.map(adjacentVertices, Element::id);
            userIds = FireflyCloseableIteratorUtils.concat(userIds, adjacentVerticesIds);
        }
        return userIds;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }

    @Override
    public void close() throws Exception {
        closeIterator();
    }
}
