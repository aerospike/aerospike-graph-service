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

package com.aerospike.firefly.io;

import com.aerospike.client.Record;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyEdgeId;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.structure.FireflyEdge.EDGE_DATA_SIZE;
import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_ADJACENT_ID_KEY;
import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_LABEL_KEY;
import static com.aerospike.firefly.structure.FireflyEdge.IN_V_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.IS_IN_SUPERNODE_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.IS_OUT_SUPERNODE_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.LABEL_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.OUT_V_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.PROPERTIES_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.TYPE_HINTS_POSITION;

public class FireflyEdgeRecord {
    private final Record edgeRecord;
    private final AerospikeConnection db;

    // Edge unique ID - List of Edge data (always used)
    private final Map<Long, List<Object>> edgeData;

    // Supernode-attached Edge data (lazy-initialized, only allocated for supernode edges)
    private Map<Long, Long> labels;
    private Map<Long, FireflyId> inVs;
    private Map<Long, FireflyId> outVs;
    private Map<Long, Map<Long, Object>> properties;
    private Map<Long, Map<Long, Object>> typeHints;
    private Map<Long, Boolean> isOutSupernodes;
    private Map<Long, Boolean> isInSupernodes;
    private Map<Long, String> edgeIdToInVHashIdKey;
    private Map<Long, String> edgeIdToOutVHashIdKey;
    private Set<String> scannedInVHashIds;
    private Set<String> scannedOutVHashIds;

    public FireflyEdgeRecord(final Record phatEdgeRecord, final AerospikeConnection db) {
        this.edgeRecord = phatEdgeRecord;
        this.db = db;
        this.edgeData = new HashMap<>(db.getConfig().phatEdgeSize);
    }

    /**
     * Allocate supernode-specific maps on first use. For non-supernode edges
     * (the common case) these maps are never needed, avoiding 10 unnecessary
     * HashMap/HashSet allocations per phat edge record.
     */
    private void ensureSupernodeMapsInitialized() {
        if (this.labels != null) {
            return;
        }
        final int size = db.getConfig().phatEdgeSize;
        this.labels = new HashMap<>(size);
        this.inVs = new HashMap<>(size);
        this.outVs = new HashMap<>(size);
        this.properties = new HashMap<>(size);
        this.typeHints = new HashMap<>(size);
        this.isOutSupernodes = new HashMap<>(size);
        this.isInSupernodes = new HashMap<>(size);
        this.edgeIdToInVHashIdKey = new HashMap<>(size);
        this.edgeIdToOutVHashIdKey = new HashMap<>(size);
        this.scannedInVHashIds = new HashSet<>(size);
        this.scannedOutVHashIds = new HashSet<>(size);
    }

    public synchronized List<Object> getEdgeData(final FireflyEdgeId edgeId) {
        final Long uniqueEdgeId = edgeId.getUniqueId();
        if (!this.edgeData.containsKey(uniqueEdgeId)) {
            final Map<ByteBuffer, Object> edgeDataMap = (Map<ByteBuffer, Object>) edgeRecord.getMap(db.getConfig().edgeDataBin);
            final Object edgeDataValue = edgeDataMap.get(edgeId.getEdgeIdBytes());
            if (edgeDataValue == null) {
                // Edge is not in this record.
                return null;
            } else if (edgeDataValue instanceof List) {
                // Edge is not attached to a supernode.
                final List<Object> edgeDataValueListFromRecord = (List<Object>) edgeDataValue;
                final List<Object> edgeDataList = new ArrayList<>(EDGE_DATA_SIZE + 2);
                for (int i = 0; i < EDGE_DATA_SIZE; i++) {
                    if (i == IN_V_POSITION || i == OUT_V_POSITION) {
                        edgeDataList.add(this.db.getIdFactory().createVertexId(edgeDataValueListFromRecord.get(i)));
                    } else {
                        edgeDataList.add(edgeDataValueListFromRecord.get(i));
                    }
                }
                edgeDataList.add(IS_IN_SUPERNODE_POSITION, false);
                edgeDataList.add(IS_OUT_SUPERNODE_POSITION, false);
                this.edgeData.put(uniqueEdgeId, edgeDataList);
                return edgeDataList;
            } else if (edgeDataValue instanceof Map) {
                // Edge is attached to a supernode,
                ensureSupernodeMapsInitialized();
                this.typeHints.put(uniqueEdgeId, (Map<Long, Object>) edgeDataValue);
                flattenSupernodeEdgeData(edgeId);
                final List<Object> edgeDataList = new ArrayList<>(EDGE_DATA_SIZE + 2);
                edgeDataList.add(LABEL_POSITION, this.labels.get(uniqueEdgeId));
                edgeDataList.add(IN_V_POSITION, this.inVs.get(uniqueEdgeId));
                edgeDataList.add(OUT_V_POSITION, this.outVs.get(uniqueEdgeId));
                edgeDataList.add(PROPERTIES_POSITION, this.properties.get(uniqueEdgeId));
                edgeDataList.add(TYPE_HINTS_POSITION, this.typeHints.get(uniqueEdgeId));
                edgeDataList.add(IS_IN_SUPERNODE_POSITION, this.isInSupernodes.get(uniqueEdgeId));
                edgeDataList.add(IS_OUT_SUPERNODE_POSITION, this.isOutSupernodes.get(uniqueEdgeId));
                this.edgeData.put(uniqueEdgeId, edgeDataList);
                return edgeDataList;
            } else {
                // This should never happen
                throw new IllegalStateException("Edge record data could not deserialize into List or Map. Please contact support.");
            }
        } else {
            return this.edgeData.get(uniqueEdgeId);
        }
    }

    public int getGeneration() {
        return this.edgeRecord.generation;
    }

    public synchronized List<FireflyEdgeId> getIndividualEdgeIdsAttachedToSupernode(final FireflyId supernodeVertexId,
                                                                                    final Direction direction,
                                                                                    final FireflyId adjacentVertexId,
                                                                                    final List<HasContainer> adjustedIdContainers) {
        ensureSupernodeMapsInitialized();
        final int adjacentPosition;
        final String supernodeDataMapBinName;
        final Map<Long, String> edgeUniqueIdToVHashIdKey;
        if (direction == Direction.BOTH) {
            // Direction.BOTH should not be propagated here and should be combined at a higher level.
            throw new IllegalStateException("Cannot get individual Edge IDs attached to a Vertex with Direction.BOTH");
        } else {
            if (direction == Direction.OUT) {
                adjacentPosition = IN_V_POSITION;
                supernodeDataMapBinName = this.db.getConfig().supernodesOutBin;
                edgeUniqueIdToVHashIdKey = this.edgeIdToOutVHashIdKey;
            } else {
                adjacentPosition = OUT_V_POSITION;
                supernodeDataMapBinName = this.db.getConfig().supernodesInBin;
                edgeUniqueIdToVHashIdKey = this.edgeIdToInVHashIdKey;
            }
        }

        final List<FireflyEdgeId> attachedEdgeIds = new ArrayList<>();
        final Map<ByteBuffer, Object> edgeDataMap = (Map<ByteBuffer, Object>) edgeRecord.getMap(db.getConfig().edgeDataBin);
        for (final Map.Entry<ByteBuffer, Object> edgeByteIdToData : edgeDataMap.entrySet()) {
            final FireflyEdgeId edgeId = this.db.getIdFactory().createEdgeId(edgeByteIdToData.getKey());
            if (edgeByteIdToData.getValue() instanceof Map) {
                // This buffers the Supernode-attached Edges of the attached vertex.
                this.flattenSupernodeDataFromVHashId(edgeId.getUniqueId(), supernodeVertexId.getKeyHashString(),
                        (Map<String, Object>) edgeRecord.getMap(supernodeDataMapBinName), direction, true);
                if (edgeUniqueIdToVHashIdKey.containsKey(edgeId.getUniqueId())) {
                    if (supernodeVertexId.getKeyHashString().equals(edgeUniqueIdToVHashIdKey.get(edgeId.getUniqueId()))) {
                        if (adjustedIdContainers != null) {
                            final List<Object> edgeData = this.getEdgeData(edgeId);
                            final FireflyId vertexId = ((FireflyId) edgeData.get(adjacentPosition));
                            if (adjustedIdContainers.stream().allMatch(c -> c.test(new ReferenceVertex(vertexId)))) {
                                attachedEdgeIds.add(edgeId);
                            }
                        } else if (adjacentVertexId == null) {
                            attachedEdgeIds.add(edgeId);
                        } else {
                            final List<Object> edgeData = this.getEdgeData(edgeId);
                            // This is needed since type hints aren't stored for Vertex IDs in Edge records, so Aerospike will return it as a long if it's numeric.
                            final Object adjacentUserId = adjacentVertexId.getUserId() instanceof Number ? ((Number) adjacentVertexId.getUserId()).longValue() : adjacentVertexId.getUserId();
                            if (((FireflyId) edgeData.get(adjacentPosition)).getUserId().equals(adjacentUserId)) {
                                attachedEdgeIds.add(edgeId);
                            }
                        }
                    }
                }
            }
        }
        return attachedEdgeIds;
    }

    public List<FireflyEdgeId> getIndividualEdgeIdsAttachedToSupernode(final FireflyId attachedVertex,
                                                                       final Direction direction) {
        return getIndividualEdgeIdsAttachedToSupernode(attachedVertex, direction, null, null);
    }

    private void flattenSupernodeEdgeData(final FireflyEdgeId edgeId) {
        boolean found = false;
        if (this.edgeIdToOutVHashIdKey.containsKey(edgeId.getUniqueId())) {
            final Map<String, Object> supernodeDataMap = (Map<String, Object>) edgeRecord.getMap(this.db.getConfig().supernodesOutBin);
            found = flattenSupernodeDataFromVHashId(edgeId.getUniqueId(),
                    this.edgeIdToOutVHashIdKey.get(edgeId.getUniqueId()),
                    supernodeDataMap, Direction.OUT, false);
        } else if (this.edgeIdToInVHashIdKey.containsKey(edgeId.getUniqueId())) {
            final Map<String, Object> supernodeDataMap = (Map<String, Object>) edgeRecord.getMap(this.db.getConfig().supernodesInBin);
            found = flattenSupernodeDataFromVHashId(edgeId.getUniqueId(),
                    this.edgeIdToInVHashIdKey.get(edgeId.getUniqueId()),
                    supernodeDataMap, Direction.IN, false);
        } else {
            final Map<String, Object> supernodeOutDataMap = (Map<String, Object>) edgeRecord.getMap(db.getConfig().supernodesOutBin);
            if (supernodeOutDataMap != null) {
                for (final String vertexHashId : supernodeOutDataMap.keySet()) {
                    found = flattenSupernodeDataFromVHashId(edgeId.getUniqueId(), vertexHashId, supernodeOutDataMap,
                            Direction.OUT, true);
                    if (found) {
                        return;
                    }
                }
            }
            final Map<String, Object> supernodeInDataMap = (Map<String, Object>) edgeRecord.getMap(db.getConfig().supernodesInBin);
            if (supernodeInDataMap != null) {
                for (final String vertexHashId : supernodeInDataMap.keySet()) {
                    found = flattenSupernodeDataFromVHashId(edgeId.getUniqueId(), vertexHashId, supernodeInDataMap,
                            Direction.IN, true);
                    if (found) {
                        return;
                    }
                }
            }
        }
        // This should never happen
        if (!found) {
            throw new IllegalStateException("Could not find Edge data from attached Vertex Hash ID. Please contact support.");
        }
    }

    private boolean flattenSupernodeDataFromVHashId(final Long edgeUniqueId, final String vertexHashId,
                                                    final Map<String, Object> supernodeDataMap,
                                                    final Direction direction, final boolean scanSupernodeIds) {
        if (supernodeDataMap == null) {
            return false;
        }
        final Long schemaLabelKey = this.db.schemaManager.getEdgePropertyRead(EDGE_SUPERNODE_LABEL_KEY);
        final Long schemaAdjacentIdKey = this.db.schemaManager.getEdgePropertyRead(EDGE_SUPERNODE_ADJACENT_ID_KEY);
        final String adjacentBinName;
        if (direction == Direction.OUT) {
            adjacentBinName = this.db.getConfig().supernodesInBin;
            if (scanSupernodeIds && this.scannedOutVHashIds.contains(vertexHashId)) {
                // We've already looked in this Vertex ID to map out Edge ID <-> IN/OUT supernode Vertex ID
                return false;
            } else {
                this.scannedOutVHashIds.add(vertexHashId);
            }
        } else {
            adjacentBinName = this.db.getConfig().supernodesOutBin;
            if (scanSupernodeIds && this.scannedInVHashIds.contains(vertexHashId)) {
                // We've already looked in this Vertex ID to map out Edge ID <-> IN/OUT supernode Vertex ID
                return false;
            } else {
                this.scannedInVHashIds.add(vertexHashId);
            }
        }
        final Map<Long, Object> propertyKeyToEdgeIdAndValuePairMap = (Map<Long, Object>) supernodeDataMap.get(vertexHashId);
        if (propertyKeyToEdgeIdAndValuePairMap == null) {
            return false;
        }
        final Map<Long, Long> edgeUniqueIdToLabel = (Map<Long, Long>) propertyKeyToEdgeIdAndValuePairMap.get(schemaLabelKey);
        if (scanSupernodeIds) {
            for (final Long edgeId : edgeUniqueIdToLabel.keySet()) {
                if (direction == Direction.OUT) {
                    this.edgeIdToOutVHashIdKey.put(edgeId, vertexHashId);
                } else {
                    this.edgeIdToInVHashIdKey.put(edgeId, vertexHashId);
                }
            }
        }
        if (!edgeUniqueIdToLabel.containsKey(edgeUniqueId)) {
            return false;
        } else {
            this.labels.put(edgeUniqueId, edgeUniqueIdToLabel.get(edgeUniqueId));
            final Map<Long, Object> edgeUniqueIdToAdjacentUserVId = (Map<Long, Object>) propertyKeyToEdgeIdAndValuePairMap.get(schemaAdjacentIdKey);
            final FireflyId adjacentVertexId = this.db.getIdFactory().createVertexId(edgeUniqueIdToAdjacentUserVId.get(edgeUniqueId));
            if (direction == Direction.OUT) {
                this.outVs.put(edgeUniqueId, this.db.getIdFactory().createVertexIdFromHash(vertexHashId));
                this.inVs.put(edgeUniqueId, adjacentVertexId);
                this.isOutSupernodes.put(edgeUniqueId, true);
                if (this.edgeIdToInVHashIdKey.containsKey(edgeUniqueId)) {
                    this.isInSupernodes.put(edgeUniqueId, true);
                } else {
                    this.isInSupernodes.put(edgeUniqueId, getIsVertexSupernode(adjacentVertexId.getKeyHashString(), adjacentBinName));
                }
            } else {
                this.inVs.put(edgeUniqueId, this.db.getIdFactory().createVertexIdFromHash(vertexHashId));
                this.outVs.put(edgeUniqueId, adjacentVertexId);
                this.isInSupernodes.put(edgeUniqueId, true);
                if (this.edgeIdToOutVHashIdKey.containsKey(edgeUniqueId)) {
                    this.isOutSupernodes.put(edgeUniqueId, true);
                } else {
                    this.isOutSupernodes.put(edgeUniqueId, getIsVertexSupernode(adjacentVertexId.getKeyHashString(), adjacentBinName));
                }
            }
            final Map<Long, Object> properties = new HashMap<>();
            for (final Map.Entry<Long, Object> propertyKeyToEdgeIdAndValuePairings : propertyKeyToEdgeIdAndValuePairMap.entrySet()) {
                final Long propertyKey = propertyKeyToEdgeIdAndValuePairings.getKey();
                if (propertyKey.equals(schemaLabelKey) || propertyKey.equals(schemaAdjacentIdKey)) {
                    continue;
                }
                final Object propertyValue = ((Map<Long, Object>) propertyKeyToEdgeIdAndValuePairings.getValue()).get(edgeUniqueId);
                if (propertyValue != null) {
                    properties.put(propertyKey, propertyValue);
                }
            }
            this.properties.put(edgeUniqueId, properties);
            return true;
        }
    }

    private boolean getIsVertexSupernode(final String vertexHashId, final String supernodeDataBinName) {
        final Map<String, Object> supernodeDataMap = (Map<String, Object>) edgeRecord.getMap(supernodeDataBinName);
        return supernodeDataMap != null && supernodeDataMap.containsKey(vertexHashId);
    }
}
