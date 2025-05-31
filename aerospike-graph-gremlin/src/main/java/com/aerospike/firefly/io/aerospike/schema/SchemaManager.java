package com.aerospike.firefly.io.aerospike.schema;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.MapExp;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_ADJACENT_ID_KEY;
import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_LABEL_KEY;
import static com.aerospike.firefly.util.Tokens.EDGE_LABEL_SCHEMA;
import static com.aerospike.firefly.util.Tokens.EDGE_PROPERTY_SCHEMA;
import static com.aerospike.firefly.util.Tokens.VERTEX_LABEL_SCHEMA;
import static com.aerospike.firefly.util.Tokens.VERTEX_PROPERTY_PROPERTY_SCHEMA;
import static com.aerospike.firefly.util.Tokens.VERTEX_PROPERTY_SCHEMA;

public class SchemaManager {
    static private final Long DUMMY_SCHEMA_LONG = -1L;
    static private final MapPolicy SCHEMA_MAP_POLICY = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.CREATE_ONLY);

    private final AerospikeConnection db;;
    private final Key vertexLabelsKey;
    private final Key vertexPropertiesKey;
    private final Key vpPropertiesKey;
    private final Key edgeLabelsKey;
    private final Key edgePropertiesKey;

    private final BiMap<String, Long> vertexLabels;
    private final BiMap<String, Long> vertexProperties;
    private final BiMap<String, Long> vpProperties;
    private final BiMap<String, Long> edgeLabels;
    private final BiMap<String, Long> edgeProperties;

    public SchemaManager(final AerospikeConnection db) {
        this.db = db;
        this.vertexLabelsKey = new Key(db.namespace, db.SCHEMA_SET, VERTEX_LABEL_SCHEMA);
        this.vertexPropertiesKey = new Key(db.namespace, db.SCHEMA_SET, VERTEX_PROPERTY_SCHEMA);
        this.vpPropertiesKey = new Key(db.namespace, db.SCHEMA_SET, VERTEX_PROPERTY_PROPERTY_SCHEMA);
        this.edgeLabelsKey = new Key(db.namespace, db.SCHEMA_SET, EDGE_LABEL_SCHEMA);
        this.edgePropertiesKey = new Key(db.namespace, db.SCHEMA_SET, EDGE_PROPERTY_SCHEMA);

        this.vertexLabels = HashBiMap.create();
        this.vertexProperties = HashBiMap.create();
        this.vpProperties = HashBiMap.create();
        this.edgeLabels = HashBiMap.create();
        this.edgeProperties = HashBiMap.create();
        this.edgeProperties.put(EDGE_SUPERNODE_LABEL_KEY, -32L);
        this.edgeProperties.put(EDGE_SUPERNODE_ADJACENT_ID_KEY, -31L);
    }

    public Long getVertexLabelWrite(final String label) {
        if (this.vertexLabels.containsKey(label)) {
            return this.vertexLabels.get(label);
        } else {
            updateVertexLabels(label);
            if (this.vertexLabels.containsKey(label)) {
                return this.vertexLabels.get(label);
            } else {
                // This should never happen.
                throw new IllegalStateException("Schema Long value did not exist for Vertex label. Please contact support.");
            }
        }
    }

    public Long getVertexLabelRead(final String label) {
        if (this.vertexLabels.containsKey(label)) {
            return this.vertexLabels.get(label);
        } else {
            updateVertexLabels(null);
        }
        final Long schemaValue = this.vertexLabels.get(label);
        return schemaValue == null ? DUMMY_SCHEMA_LONG : schemaValue;
    }

    public String getVertexLabelString(final Long storageLabel) {
        if (this.vertexLabels.inverse().containsKey(storageLabel)) {
            return this.vertexLabels.inverse().get(storageLabel);
        } else {
            updateVertexLabels(null);
            if (this.vertexLabels.inverse().containsKey(storageLabel)) {
                return this.vertexLabels.inverse().get(storageLabel);
            } else {
                // This should never happen.
                throw new IllegalStateException("Schema String value did not exist for Vertex label storage type. Please contact support.");
            }
        }
    }

    public Long getVertexPropertyWrite(final String propertyKey) {
        if (this.vertexProperties.containsKey(propertyKey)) {
            return this.vertexProperties.get(propertyKey);
        } else {
            updateVertexProperties(propertyKey);
            if (this.vertexProperties.containsKey(propertyKey)) {
                return this.vertexProperties.get(propertyKey);
            } else {
                // This should never happen.
                throw new IllegalStateException("Schema Long value did not exist for Vertex property. Please contact support.");
            }
        }
    }

    public Long getVertexPropertyRead(final String propertyKey) {
        if (this.vertexProperties.containsKey(propertyKey)) {
            return this.vertexProperties.get(propertyKey);
        } else {
            updateVertexProperties(null);
        }
        final Long schemaValue = this.vertexProperties.get(propertyKey);
        return schemaValue == null ? DUMMY_SCHEMA_LONG : schemaValue;
    }

    public String getVertexPropertyString(final Long storagePropertyKey) {
        if (this.vertexProperties.inverse().containsKey(storagePropertyKey)) {
            return this.vertexProperties.inverse().get(storagePropertyKey);
        } else {
            updateVertexProperties(null);
            if (this.vertexProperties.inverse().containsKey(storagePropertyKey)) {
                return this.vertexProperties.inverse().get(storagePropertyKey);
            } else {
                // This should never happen.
                throw new IllegalStateException("Schema String value did not exist for Vertex property storage type. Please contact support.");
            }
        }
    }

    public void populateVertexPropertyStringMapToSchemaMap(final Map<String, ?> vpStringMap,
                                                                            final Map<Long, Object> outMap) {
        for (final Map.Entry<String, ?> entry : vpStringMap.entrySet()) {
            final Long schemaKey = getVertexPropertyWrite(entry.getKey());
            outMap.put(schemaKey, entry.getValue());
        }
    }

    public void populateVertexPropertySchemaMapToStringMap(final Map<Long, ?> vpSchemaMap,
                                                           final Map<String, Object> outMap) {
        for (final Map.Entry<Long, ?> entry : vpSchemaMap.entrySet()) {
            final String key = getVertexPropertyString(entry.getKey());
            outMap.put(key, entry.getValue());
        }

    }

    public Long getVpPropertyWrite(final String propertyKey) {
        if (this.vpProperties.containsKey(propertyKey)) {
            return this.vpProperties.get(propertyKey);
        } else {
            updateVpProperties(propertyKey);
            if (this.vpProperties.containsKey(propertyKey)) {
                return this.vpProperties.get(propertyKey);
            } else {
                // This should never happen.
                throw new IllegalStateException("Schema Long value did not exist for VP property. Please contact support.");
            }
        }
    }

    public Long getVpPropertyRead(final String propertyKey) {
        if (this.vpProperties.containsKey(propertyKey)) {
            return this.vpProperties.get(propertyKey);
        } else {
            updateVpProperties(null);
        }
        final Long schemaValue = this.vpProperties.get(propertyKey);
        return schemaValue == null ? DUMMY_SCHEMA_LONG : schemaValue;
    }

    public String getVpPropertyString(final Long storagePropertyKey) {
        if (this.vpProperties.inverse().containsKey(storagePropertyKey)) {
            return this.vpProperties.inverse().get(storagePropertyKey);
        } else {
            updateVpProperties(null);
            if (this.vpProperties.inverse().containsKey(storagePropertyKey)) {
                return this.vpProperties.inverse().get(storagePropertyKey);
            } else {
                // This should never happen.
                throw new IllegalStateException("Schema String value did not exist for VP property storage type. Please contact support.");
            }
        }
    }

    public void populateVpPropertyStringMapToSchemaMap(final Map<String, ?> vpPropertyStringMap,
                                                       final Map<Long, Object> outMap) {
        for (final Map.Entry<String, ?> entry : vpPropertyStringMap.entrySet()) {
            final Long schemaKey = getVpPropertyWrite(entry.getKey());
            outMap.put(schemaKey, entry.getValue());
        }
    }

    public void populateVpPropertySchemaMapToStringMap(final Map<Long, ?> vpPropertySchemaMap,
                                                       final Map<String, Object> outMap) {
        if (vpPropertySchemaMap != null) {
            for (final Map.Entry<Long, ?> entry : vpPropertySchemaMap.entrySet()) {
                final String key = getVpPropertyString(entry.getKey());
                outMap.put(key, entry.getValue());
            }
        }
    }

    public void populateVertexVpPropertySchemaMapToStringMap(
            final Map<Object, Map<Long, Object>> vertexVpPropertyMap,
            final Map<Object, Map<String, Object>> outMap) {
        for (final Map.Entry<Object, Map<Long, Object>> entry : vertexVpPropertyMap.entrySet()) {
            final Map<String, Object> vpPropertyStringMap = new TreeMap<>();
            populateVpPropertySchemaMapToStringMap(entry.getValue(), vpPropertyStringMap);
            outMap.put(entry.getKey(), vpPropertyStringMap);
        }
    }

    public Long getEdgeLabelWrite(final String label) {
        if (this.edgeLabels.containsKey(label)) {
            return this.edgeLabels.get(label);
        } else {
            updateEdgeLabels(label);
            if (this.edgeLabels.containsKey(label)) {
                return this.edgeLabels.get(label);
            } else {
                // This should never happen.
                throw new IllegalStateException("Schema Long value did not exist for Edge label. Please contact support.");
            }
        }
    }

    public Long getEdgeLabelRead(final String label) {
        if (this.edgeLabels.containsKey(label)) {
            return this.edgeLabels.get(label);
        } else {
            updateEdgeLabels(null);
        }
        final Long schemaValue = this.edgeLabels.get(label);
        return schemaValue == null ? DUMMY_SCHEMA_LONG : schemaValue;
    }

    public String getEdgeLabelString(final Long storageLabel) {
        if (this.edgeLabels.inverse().containsKey(storageLabel)) {
            return this.edgeLabels.inverse().get(storageLabel);
        } else {
            updateEdgeLabels(null);
            if (this.edgeLabels.inverse().containsKey(storageLabel)) {
                return this.edgeLabels.inverse().get(storageLabel);
            } else {
                // This should never happen.
                throw new IllegalStateException("Schema String value did not exist for Edge label storage type. Please contact support.");
            }
        }
    }

    public Long getEdgePropertyWrite(final String propertyKey) {
        if (this.edgeProperties.containsKey(propertyKey)) {
            return this.edgeProperties.get(propertyKey);
        } else {
            updateEdgeProperties(propertyKey);
            if (this.edgeProperties.containsKey(propertyKey)) {
                return this.edgeProperties.get(propertyKey);
            } else {
                // This should never happen.
                throw new IllegalStateException("Schema Long value did not exist for Edge property. Please contact support.");
            }
        }
    }

    public Long getEdgePropertyRead(final String propertyKey) {
        if (this.edgeProperties.containsKey(propertyKey)) {
            return this.edgeProperties.get(propertyKey);
        } else {
            updateEdgeProperties(null);
        }
        final Long schemaValue = this.edgeProperties.get(propertyKey);
        return schemaValue == null ? DUMMY_SCHEMA_LONG : schemaValue;
    }

    public String getEdgePropertyString(final Long storagePropertyKey) {
        if (this.edgeProperties.inverse().containsKey(storagePropertyKey)) {
            return this.edgeProperties.inverse().get(storagePropertyKey);
        } else {
            updateEdgeProperties(null);
            if (this.edgeProperties.inverse().containsKey(storagePropertyKey)) {
                return this.edgeProperties.inverse().get(storagePropertyKey);
            } else {
                // This should never happen.
                throw new IllegalStateException("Schema String value did not exist for Edge property storage type. Please contact support.");
            }
        }
    }

    public void populateEdgePropertyStringMapToSchemaMap(final Map<String, ?> edgePropertyStringMap,
                                                         final Map<Long, Object> outMap) {
        for (final Map.Entry<String, ?> entry : edgePropertyStringMap.entrySet()) {
            final Long schemaKey = getEdgePropertyRead(entry.getKey());
            outMap.put(schemaKey, entry.getValue());
        }
    }

    public void populateEdgePropertySchemaMapToStringMap(final Map<Long, ?> edgePropertySchemaMap,
                                                         final Map<String, Object> outMap) {
        for (final Map.Entry<Long, ?> entry : edgePropertySchemaMap.entrySet()) {
            final String key = getEdgePropertyString(entry.getKey());
            outMap.put(key, entry.getValue());
        }
    }

    public void updateAll() {
        updateVertexLabels(null);
        updateVertexProperties(null);
        updateVpProperties(null);
        updateEdgeLabels(null);
        updateEdgeProperties(null);
    }

    private void updateVertexLabels(final String label) {
        synchronized (this.vertexLabelsKey) {
            final Map<String, Long> diskSchema = updateSchemaMap(label, this.vertexLabelsKey);
            this.vertexLabels.putAll(diskSchema);
        }
    }

    private void updateVertexProperties(final String propertyKey) {
        synchronized (this.vertexPropertiesKey) {
            final Map<String, Long> diskSchema = updateSchemaMap(propertyKey, this.vertexPropertiesKey);
            this.vertexProperties.putAll(diskSchema);
        }
    }

    private void updateVpProperties(final String propertyKey) {
        synchronized (this.vpPropertiesKey) {
            final Map<String, Long> diskSchema = updateSchemaMap(propertyKey, this.vpPropertiesKey);
            this.vpProperties.putAll(diskSchema);
        }
    }

    private void updateEdgeLabels(final String label) {
        synchronized (this.edgeLabelsKey) {
            final Map<String, Long> diskSchema = updateSchemaMap(label, this.edgeLabelsKey);
            this.edgeLabels.putAll(diskSchema);
        }
    }

    private void updateEdgeProperties(final String propertyKey) {
        synchronized (this.edgePropertiesKey) {
            final Map<String, Long> diskSchema = updateSchemaMap(propertyKey, this.edgePropertiesKey);
            this.edgeProperties.putAll(diskSchema);
        }
    }

    private Map<String, Long> updateSchemaMap(final String schemaKey, final Key recordKey) {
        final List<Operation> operations = new ArrayList<>();
        if (schemaKey != null) {
            final Expression incrementCounter = Exp.build(Exp.cond(
                    Exp.not(MapExp.getByKey(MapReturnType.EXISTS, Exp.Type.BOOL, Exp.val(schemaKey), Exp.mapBin(this.db.SCHEMA_BIN))),
                    Exp.add(Exp.bin(this.db.COUNTER_BIN, Exp.Type.INT), Exp.val(1)),
                    Exp.unknown()
            ));
            final Operation incrementCounterOp = ExpOperation.write(this.db.COUNTER_BIN, incrementCounter, ExpWriteFlags.EVAL_NO_FAIL);
            operations.add(incrementCounterOp);
            final Expression addSchemaPair = Exp.build(Exp.cond(
                    Exp.not(MapExp.getByKey(MapReturnType.EXISTS, Exp.Type.BOOL, Exp.val(schemaKey), Exp.mapBin(this.db.SCHEMA_BIN))),
                    MapExp.put(SCHEMA_MAP_POLICY, Exp.val(schemaKey), Exp.sub(Exp.bin(this.db.COUNTER_BIN, Exp.Type.INT), Exp.val(1)), Exp.mapBin(this.db.SCHEMA_BIN)),
                    Exp.unknown()
            ));
            final Operation addSchemaPairOp = ExpOperation.write(this.db.SCHEMA_BIN, addSchemaPair, ExpWriteFlags.EVAL_NO_FAIL);
            operations.add(addSchemaPairOp);
        }
        final Operation readSchema = Operation.get(this.db.SCHEMA_BIN);
        operations.add(readSchema);

        final Record record = this.db.writeOperate(null, recordKey, operations.toArray(new Operation[0]));
        final Object schemaResult = record.getValue(this.db.SCHEMA_BIN);
        if (schemaResult instanceof Map) {
            // This was a read-only operation
            return (Map<String, Long>) schemaResult;
        } else if (schemaResult instanceof List) {
            // Checked or created a new schema pair
            return (Map<String, Long>) ((List<?>) schemaResult).get(1);
        } else {
            // This should never happen
            throw new IllegalStateException("Schema mapping operation returned a type that is not Map or List. Please contact support.");
        }
    }
}
