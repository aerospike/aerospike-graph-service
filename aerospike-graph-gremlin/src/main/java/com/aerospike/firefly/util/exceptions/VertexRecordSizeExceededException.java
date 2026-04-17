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

package com.aerospike.firefly.util.exceptions;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;

import java.util.List;
import java.util.Map;

public class VertexRecordSizeExceededException extends AerospikeGraphRecordSizeExceededException {
    private static final String ADD_ECACHE_BASE_MESSAGE = "Record size exceeded for Vertex with ID %s when writing to Edge Cache with Edge ID %s";
    private static final String ADD_VERTEX_PROPERTY_BASE_MESSAGE = "Record size exceeded for Vertex with ID %s when writing Vertex Property key %s";
    private static final String ADD_VP_PROPERTY_BASE_MESSAGE = "Record size exceeded for Vertex with ID %s when writing Property key %s for Vertex Property %s";
    private final String message;
    public final long inEdgeCount;
    public final long outEdgeCount;
    public final long vertexPropertyCount;
    public final long vpPropertyCount;

    private VertexRecordSizeExceededException(final AerospikeException cause, final String message,
                                              final VertexRecordMetrics metrics) {
        super(cause);
        this.message = message;
        this.inEdgeCount = metrics.inEdgeCount;
        this.outEdgeCount = metrics.outEdgeCount;
        this.vertexPropertyCount = metrics.vertexPropertyCount;
        this.vpPropertyCount = metrics.vpPropertyCount;
    }

    @Override
    public String getMessage() {
        return this.message;
    }

    public static VertexRecordSizeExceededException fromAddingToEdgeCache(final AerospikeException cause,
                                                                          final AerospikeConnection db,
                                                                          final Record record, final FireflyId vertexId,
                                                                          final FireflyId edgeId) {
        final String baseMessage = String.format(ADD_ECACHE_BASE_MESSAGE, vertexId.getUserId(), edgeId.getUserId());
        final VertexRecordMetrics metrics = new VertexRecordMetrics(record, db);
        final String message = buildMessage(baseMessage, metrics);
        return new VertexRecordSizeExceededException(cause, message, metrics);
    }

    public static VertexRecordSizeExceededException fromAddingVertexProperty(final AerospikeException cause,
                                                                             final AerospikeConnection db,
                                                                             final Record record,
                                                                             final FireflyId vertexId,
                                                                             final String key) {
        final String baseMessage = String.format(ADD_VERTEX_PROPERTY_BASE_MESSAGE, vertexId.getUserId(), key);
        final VertexRecordMetrics metrics = new VertexRecordMetrics(record, db);
        final String message = buildMessage(baseMessage, metrics);
        return new VertexRecordSizeExceededException(cause, message, metrics);
    }

    public static VertexRecordSizeExceededException fromAddingVpProperty(final AerospikeException cause,
                                                                         final AerospikeConnection db,
                                                                         final Record record, final FireflyId vertexId,
                                                                         final String vpKey,
                                                                         final String key) {
        final String baseMessage = String.format(ADD_VP_PROPERTY_BASE_MESSAGE, vertexId.getUserId(), key, vpKey);
        final VertexRecordMetrics metrics = new VertexRecordMetrics(record, db);
        final String message = buildMessage(baseMessage, metrics);
        return new VertexRecordSizeExceededException(cause, message, metrics);
    }

    public static Record getRelevantVertexBins(final AerospikeConnection db, final Key key) {
        final Operation getInEdges = Operation.get(db.getConfig().inEdgesBin);
        final Operation getOutEdges = Operation.get(db.getConfig().outEdgesBin);
        final Operation getVpCount = Operation.get(db.getConfig().vertexPropertyTHBin);
        final Operation getVpProperties = Operation.get(db.getConfig().vpPropertyBin);
        return db.readOperate(null, key, getInEdges, getOutEdges, getVpCount, getVpProperties);
    }

    private static String buildMessage(final String baseMessage, final VertexRecordMetrics metrics) {
        final StringBuilder builder = new StringBuilder(baseMessage);
        builder.append("\nIN Edge Cache size: ");
        builder.append(metrics.inEdgeCount);
        builder.append("\nOUT Edge Cache size: ");
        builder.append(metrics.outEdgeCount);
        builder.append("\nVertex Property count: ");
        builder.append(metrics.vertexPropertyCount);
        builder.append("\nVertex Property Properties count: ");
        builder.append(metrics.vpPropertyCount);
        return builder.toString();
    }

    private static class VertexRecordMetrics {
        private final long inEdgeCount;
        private final long outEdgeCount;
        private final long vertexPropertyCount;
        private final long vpPropertyCount;

        private VertexRecordMetrics(final Record record, final AerospikeConnection db) {
            this.inEdgeCount = getCountFromNestedList((Map) record.getMap(db.getConfig().inEdgesBin));
            this.outEdgeCount = getCountFromNestedList((Map) record.getMap(db.getConfig().outEdgesBin));
            this.vertexPropertyCount = getCountFromNestedMap((Map) record.getMap(db.getConfig().vertexPropertyTHBin));
            this.vpPropertyCount = getCountFromNestedNestedMap((Map) record.getMap(db.getConfig().vpPropertyBin));
        }

        private long getCountFromNestedList(final Map<?, List<?>> nestedList) {
            long count = 0;
            for (final List<?> list : nestedList.values()) {
                count += list.size();
            }
            return count;
        }

        private long getCountFromNestedMap(final Map<?, Map<?, ?>> nestedMap) {
            long count = 0;
            for (final Map<?, ?> map : nestedMap.values()) {
                count += map.size();
            }
            return count;
        }

        private long getCountFromNestedNestedMap(final Map<?, Map<?, Map<?, ?>>> nestedNestedMap) {
            long count = 0;
            for (final Map<?, Map<?, ?>> map : nestedNestedMap.values()) {
                for (final Map<?, ?> innerMap : map.values()) {
                    count += innerMap.size();
                }
            }
            return count;
        }
    }
}
