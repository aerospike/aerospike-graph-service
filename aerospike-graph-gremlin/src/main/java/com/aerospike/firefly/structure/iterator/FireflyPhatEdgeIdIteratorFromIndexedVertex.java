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

package com.aerospike.firefly.structure.iterator;

import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.FireflyEdgeRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyEdgeFactory;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyEdgeId;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FireflyPhatEdgeIdIteratorFromIndexedVertex extends FireflyPhatEdgeIdIteratorFromVertex {
    private final FireflyId adjacentVertexId;
    private final List<HasContainer> adjustedIdContainers;
    private final Map<FireflyId, FireflyEdge> edgeCache;
    private final FireflyGraph graph;

    /**
     * Wrapper iterator for converting KeyRecord of Phat Edges fetched via an Adjacency Index into all of its contained
     * edges' FireflyIds that are attached to a specified Vertex.
     *
     * @param keyRecordIterator KeyRecord iterator to wrap.
     * @param db                AerospikeConnection instance.
     * @param direction         The Direction from the Vertex.
     * @param vertexId          The ID of the Vertex.
     * @param labels            The labels of the Edge to filter on.
     */
    public FireflyPhatEdgeIdIteratorFromIndexedVertex(final Iterator<KeyRecord> keyRecordIterator,
                                                      final AerospikeConnection db,
                                                      final Direction direction,
                                                      final FireflyId vertexId,
                                                      final Set<String> labels,
                                                      final OutputType outputType,
                                                      final List<HasContainer> adjustedIdContainers) {
        super(keyRecordIterator, db, direction, vertexId, labels, outputType);
        this.adjacentVertexId = null;
        this.adjustedIdContainers = adjustedIdContainers;
        this.edgeCache = null;
        this.graph = null;
    }

    public FireflyPhatEdgeIdIteratorFromIndexedVertex(final Iterator<KeyRecord> keyRecordIterator,
                                                      final FireflyGraph graph,
                                                      final Direction direction,
                                                      final FireflyId vertexId,
                                                      final Set<String> labels,
                                                      final OutputType outputType,
                                                      final FireflyId adjacentVertexId,
                                                      final Map<FireflyId, FireflyEdge> edgeCache) {
        super(keyRecordIterator, graph.getBaseGraph(), direction, vertexId, labels, outputType);
        this.adjacentVertexId = adjacentVertexId;
        this.adjustedIdContainers = null;
        this.edgeCache = edgeCache;
        this.graph = graph;
    }

    @Override
    protected List<FireflyEdgeId> getIndividualEdgeIdsAttachedToVertex(final FireflyEdgeRecord record, final Direction direction) {
        final List<FireflyEdgeId> ids = record.getIndividualEdgeIdsAttachedToSupernode(this.vertexId, direction, this.adjacentVertexId, this.adjustedIdContainers);

        if (edgeCache != null) {
            for (final FireflyEdgeId edgeId : ids) {
                edgeCache.put(edgeId, FireflyEdgeFactory.create(edgeId, record, graph));
            }
        }
        return ids;
    }
}
