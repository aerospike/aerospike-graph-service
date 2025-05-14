package com.aerospike.firefly.io;

import com.aerospike.client.Record;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyEdgeId;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_IN_KEY;
import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_LABEL_KEY;
import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_OUT_KEY;
import static com.aerospike.firefly.structure.FireflyEdge.IN_V_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.LABEL_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.OUT_V_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.PROPERTIES_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.TYPE_HINTS_POSITION;

public class FireflyEdgeRecord {
    private AtomicBoolean isExpanded = new AtomicBoolean(false);
    private final Record edgeRecord;
    private final AerospikeConnection db;
    private final List<FireflyEdgeId> edgeIds;
    private final Map<Long, String> labels;
    private final Map<Long, FireflyId> inVs;
    private final Map<Long, FireflyId> outVs;
    private final Map<Long, Map<String, Object>> properties;
    private final Map<Long, Map<String, Object>> typeHints;
    private final Map<Long, Boolean> isOutSupernodes;
    private final Map<Long, Boolean> isInSupernodes;

    public FireflyEdgeRecord(final Record phatEdgeRecord, final AerospikeConnection db) {
        this.edgeRecord = phatEdgeRecord;
        this.db = db;
        this.edgeIds = new ArrayList<>(db.PHAT_EDGE_SIZE);
        this.labels = new HashMap<>(db.PHAT_EDGE_SIZE);
        this.inVs = new HashMap<>(db.PHAT_EDGE_SIZE);
        this.outVs = new HashMap<>(db.PHAT_EDGE_SIZE);
        this.properties = new HashMap<>(db.PHAT_EDGE_SIZE);
        this.typeHints = new HashMap<>(db.PHAT_EDGE_SIZE);
        this.isOutSupernodes = new HashMap<>(db.PHAT_EDGE_SIZE);
        this.isInSupernodes = new HashMap<>(db.PHAT_EDGE_SIZE);
    }

    public List<FireflyEdgeId> getEdgeIds() {
        this.expand();
        return this.edgeIds;
    }

    public String getLabel(final FireflyEdgeId edgeId) {
        return this.getLabel(edgeId.getUniqueId());
    }

    public String getLabel(final Long uniqueEdgeId) {
        this.expand();
        return this.labels.get(uniqueEdgeId);
    }

    public FireflyId getInV(final FireflyEdgeId edgeId) {
        return this.getInV(edgeId.getUniqueId());
    }

    public FireflyId getInV(final Long uniqueEdgeId) {
        this.expand();
        return this.inVs.get(uniqueEdgeId);
    }

    public FireflyId getOutV(final FireflyEdgeId edgeId) {
        return this.getOutV(edgeId.getUniqueId());
    }

    public FireflyId getOutV(final Long uniqueEdgeId) {
        this.expand();
        return this.outVs.get(uniqueEdgeId);
    }

    public Map<String, Object> getProperties(final FireflyEdgeId edgeId) {
        return this.getProperties(edgeId.getUniqueId());
    }

    public Map<String, Object> getProperties(final Long uniqueEdgeId) {
        this.expand();
        return this.properties.get(uniqueEdgeId);
    }

    public Map<String, Object> getTypeHints(final FireflyEdgeId edgeId) {
        return this.getTypeHints(edgeId.getUniqueId());
    }

    public Map<String, Object> getTypeHints(final Long uniqueEdgeId) {
        this.expand();
        return this.typeHints.get(uniqueEdgeId);
    }

    public boolean getIsOutSupernode(final FireflyEdgeId edgeId) {
        return this.getIsOutSupernode(edgeId.getUniqueId());
    }

    public boolean getIsOutSupernode(final Long uniqueEdgeId) {
        this.expand();
        return this.isOutSupernodes.get(uniqueEdgeId);
    }

    public boolean getIsInSupernode(final FireflyEdgeId edgeId) {
        return this.getIsInSupernode(edgeId.getUniqueId());
    }

    public boolean getIsInSupernode(final Long uniqueEdgeId) {
        this.expand();
        return this.isInSupernodes.get(uniqueEdgeId);
    }

    public int getGeneration() {
        return this.edgeRecord.generation;
    }

    private synchronized void expand() {
        if (!this.isExpanded.getAndSet(true)) {
            final Map<ByteBuffer, Object> edgeDataMap = (Map<ByteBuffer, Object>) edgeRecord.getMap(db.EDGE_DATA_BIN);
            for (final Map.Entry<ByteBuffer, Object> edgeData : edgeDataMap.entrySet()) {
                final FireflyPhatEdgeId edgeId = this.db.getIdFactory().createEdgeId(edgeData.getKey());
                this.edgeIds.add(edgeId);
                final Long uniqueId = edgeId.getUniqueId();
                if (edgeData.getValue() instanceof List) {
                    // Not attached to a supernode
                    final List<Object> edgeDataList = (List<Object>) edgeData.getValue();
                    this.labels.put(uniqueId, (String) edgeDataList.get(LABEL_POSITION));
                    this.inVs.put(uniqueId, this.db.getIdFactory().createVertexId(edgeDataList.get(IN_V_POSITION)));
                    this.outVs.put(uniqueId, this.db.getIdFactory().createVertexId(edgeDataList.get(OUT_V_POSITION)));
                    this.properties.put(uniqueId, (Map<String, Object>) edgeDataList.get(PROPERTIES_POSITION));
                    this.typeHints.put(uniqueId, (Map<String, Object>) edgeDataList.get(TYPE_HINTS_POSITION));
                    this.isOutSupernodes.put(uniqueId, false);
                    this.isInSupernodes.put(uniqueId, false);
                } else if (edgeData.getValue() instanceof Map) {
                    // Attached to a supernode
                    this.typeHints.put(uniqueId, (Map<String, Object>) edgeData.getValue());
                    this.properties.put(uniqueId, new HashMap<>());
                } else {
                    // This should never happen
                    throw new IllegalStateException("Edge record data could not deserialize into List or Map. Please contact support.");
                }
            }

            final Map<String, Object> supernodeOutMap = (Map<String, Object>) edgeRecord.getMap(this.db.SUPERNODES_OUT_BIN);
            if (supernodeOutMap != null) {
                for (final Map.Entry<String, Object> vHashIdToPropertyMap : supernodeOutMap.entrySet()) {
                    final FireflyId outVId = this.db.getIdFactory().createVertexIdFromHash(vHashIdToPropertyMap.getKey());
                    final Map<String, Object> propertyKeyToEdgeIdAndValuePairMap = (Map<String, Object>) vHashIdToPropertyMap.getValue();
                    for (final Map.Entry<String, Object> propertyKeyToEdgeIdAndValuePairings : propertyKeyToEdgeIdAndValuePairMap.entrySet()) {
                        final String propertyKey = propertyKeyToEdgeIdAndValuePairings.getKey();
                        final Map<Long, Object> edgeUniqueIdtoPropertyValueMap = (Map<Long, Object>) propertyKeyToEdgeIdAndValuePairings.getValue();
                        if (propertyKey.equals(EDGE_SUPERNODE_LABEL_KEY)) {
                            // Label
                            for (final Map.Entry<Long, Object> edgeUniqueIdToLabel : edgeUniqueIdtoPropertyValueMap.entrySet()) {
                                this.labels.put(edgeUniqueIdToLabel.getKey(), (String) edgeUniqueIdToLabel.getValue());
                            }
                        } else if (propertyKey.equals(EDGE_SUPERNODE_IN_KEY)) {
                            // Adjacent (IN) Vertex
                            for (final Map.Entry<Long, Object> edgeUniqueIdToInVUserId : edgeUniqueIdtoPropertyValueMap.entrySet()) {
                                this.inVs.put(edgeUniqueIdToInVUserId.getKey(), this.db.getIdFactory().createVertexId(edgeUniqueIdToInVUserId.getValue()));
                                // Also put the other attached Vertex here
                                this.outVs.put(edgeUniqueIdToInVUserId.getKey(), outVId);
                                this.isOutSupernodes.put(edgeUniqueIdToInVUserId.getKey(), true);
                            }
                        } else {
                            // Normal property
                            for (final Map.Entry<Long, Object> edgeUniqueIdToValue : edgeUniqueIdtoPropertyValueMap.entrySet()) {
                                final Map<String, Object> properties = this.properties.get(edgeUniqueIdToValue.getKey());
                                properties.put(propertyKey, edgeUniqueIdToValue.getValue());
                            }
                        }
                    }
                }
            }

            final Map<String, Object> supernodeInMap = (Map<String, Object>) edgeRecord.getMap(this.db.SUPERNODES_IN_BIN);
            if (supernodeInMap != null) {
                for (final Map.Entry<String, Object> vHashIdToPropertyMap : supernodeInMap.entrySet()) {
                    final FireflyId inVId = this.db.getIdFactory().createVertexIdFromHash(vHashIdToPropertyMap.getKey());
                    final Map<String, Object> propertyKeyToEdgeIdAndValuePairMap = (Map<String, Object>) vHashIdToPropertyMap.getValue();
                    for (final Map.Entry<String, Object> propertyKeyToEdgeIdAndValuePairings : propertyKeyToEdgeIdAndValuePairMap.entrySet()) {
                        final String propertyKey = propertyKeyToEdgeIdAndValuePairings.getKey();
                        final Map<Long, Object> edgeUniqueIdtoPropertyValueMap = (Map<Long, Object>) propertyKeyToEdgeIdAndValuePairings.getValue();
                        if (propertyKey.equals(EDGE_SUPERNODE_LABEL_KEY)) {
                            // Label
                            for (final Map.Entry<Long, Object> edgeUniqueIdToLabel : edgeUniqueIdtoPropertyValueMap.entrySet()) {
                                this.labels.putIfAbsent(edgeUniqueIdToLabel.getKey(), (String) edgeUniqueIdToLabel.getValue());
                            }
                        } else if (propertyKey.equals(EDGE_SUPERNODE_OUT_KEY)) {
                            // Adjacent (OUT) Vertex
                            for (final Map.Entry<Long, Object> edgeUniqueIdToInVUserId : edgeUniqueIdtoPropertyValueMap.entrySet()) {
                                if (!this.outVs.containsKey(edgeUniqueIdToInVUserId.getKey())) {
                                    this.outVs.put(edgeUniqueIdToInVUserId.getKey(), this.db.getIdFactory().createVertexId(edgeUniqueIdToInVUserId.getValue()));
                                }
                                // Also put the other attached Vertex here
                                this.inVs.putIfAbsent(edgeUniqueIdToInVUserId.getKey(), inVId);
                                this.isInSupernodes.put(edgeUniqueIdToInVUserId.getKey(), true);
                            }
                        } else {
                            // Normal property
                            for (final Map.Entry<Long, Object> edgeUniqueIdToValue : edgeUniqueIdtoPropertyValueMap.entrySet()) {
                                final Map<String, Object> properties = this.properties.get(edgeUniqueIdToValue.getKey());
                                properties.putIfAbsent(propertyKey, edgeUniqueIdToValue.getValue());
                            }
                        }
                    }
                }
            }

            // Fill out the rest of the supernode statuses
            for (final Long uniqueId : this.typeHints.keySet()) {
                this.isInSupernodes.putIfAbsent(uniqueId, false);
                this.isOutSupernodes.putIfAbsent(uniqueId, false);
            }
        }
    }
}
