package com.aerospike.firefly.io.aerospike.schema;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.util.exceptions.AerospikeGraphElementNotFoundException;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.aerospike.firefly.util.exceptions.GraphError;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_ADJACENT_ID_KEY;
import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_LABEL_KEY;
import static com.aerospike.firefly.util.Tokens.EDGE_LABEL_SCHEMA;
import static com.aerospike.firefly.util.Tokens.EDGE_PROPERTY_SCHEMA;
import static com.aerospike.firefly.util.Tokens.VERTEX_LABEL_SCHEMA;
import static com.aerospike.firefly.util.Tokens.VERTEX_PROPERTY_PROPERTY_SCHEMA;
import static com.aerospike.firefly.util.Tokens.VERTEX_PROPERTY_SCHEMA;

public class SchemaManager {
    static private final Logger LOG = LoggerFactory.getLogger(SchemaManager.class);

    private static final Set<Integer> KEY_EXISTS_CODE = Set.of(ResultCode.KEY_EXISTS_ERROR);
    private static final Set<Integer> ELEMENT_NOT_FOUND_CODE = Set.of(GraphError.ELEMENT_NOT_FOUND.code);

    private static final Long DUMMY_SCHEMA_LONG = -100L;
    private static final MapPolicy SCHEMA_MAP_POLICY = new MapPolicy(MapOrder.UNORDERED, MapWriteFlags.CREATE_ONLY);

    private final AerospikeConnection db;;
    private final Key vertexLabelsKey;
    private final Key vertexPropertiesKey;
    private final Key vpPropertiesKey;
    private final Key edgeLabelsKey;
    private final Key edgePropertiesKey;

    private final BiMap<String, Long> vertexLabels;
    private final ThreadLocal<Set<String>> missingVertexLabels = ThreadLocal.withInitial(HashSet::new);
    private final BiMap<String, Long> vertexProperties;
    private final ThreadLocal<Set<String>> missingVertexProperties = ThreadLocal.withInitial(HashSet::new);
    private final BiMap<String, Long> vpProperties;
    private final ThreadLocal<Set<String>> missingVpProperties = ThreadLocal.withInitial(HashSet::new);
    private final BiMap<String, Long> edgeLabels;
    private final ThreadLocal<Set<String>> missingEdgeLabels = ThreadLocal.withInitial(HashSet::new);
    private final BiMap<String, Long> edgeProperties;
    private final ThreadLocal<Set<String>> missingEdgeProperties = ThreadLocal.withInitial(HashSet::new);

    private final Map<Key, AtomicBoolean> initializedMap = new HashMap<>();
    private final Map<Key, String> readableNames = new HashMap();

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

        initializedMap.put(this.vertexLabelsKey, new AtomicBoolean(false));
        initializedMap.put(this.vertexPropertiesKey, new AtomicBoolean(false));
        initializedMap.put(this.vpPropertiesKey, new AtomicBoolean(false));
        initializedMap.put(this.edgeLabelsKey, new AtomicBoolean(false));
        initializedMap.put(this.edgePropertiesKey, new AtomicBoolean(false));

        readableNames.put(this.vertexLabelsKey, "Vertex label");
        readableNames.put(this.vertexPropertiesKey, "Vertex property");
        readableNames.put(this.vpPropertiesKey, "Vertex properties property");
        readableNames.put(this.edgeLabelsKey, "Edge label");
        readableNames.put(this.edgePropertiesKey, "Edge property");
    }

    public Long getVertexLabelWrite(final String label) {
        for (int attempts = 0; attempts < 10; attempts++) {
            final Long schema = this.vertexLabels.get(label);
            if (schema == null) {
                updateVertexLabels(label);
            } else {
                return schema;
            }
        }
        // This should never happen.
        throw new IllegalStateException("Schema Long value did not exist for Vertex label. Please contact support.");
    }

    public Long getVertexLabelRead(final String label) {
        // Reads need to be fully synchronized because can't rely on null to indicate concurrency issue
        synchronized (this.vertexLabelsKey) {
            if (this.vertexLabels.containsKey(label)) {
                return this.vertexLabels.get(label);
            } else {
                if (!missingVertexLabels.get().contains(label)) {
                    updateVertexLabels(null);
                    missingVertexLabels.get().add(label);
                }
            }
            final Long schemaValue = this.vertexLabels.get(label);
            return schemaValue == null ? DUMMY_SCHEMA_LONG : schemaValue;
        }
    }

    public String getVertexLabelString(final Long storageLabel) {
        for (int attempts = 0; attempts < 10; attempts++) {
            final String label =  this.vertexLabels.inverse().get(storageLabel);
            if (label == null) {
                updateVertexLabels(null);
            } else {
                return label;
            }
        }
        // This should never happen.
        throw new IllegalStateException("Schema String value did not exist for Vertex label storage type. Please contact support.");
    }

    public Long getVertexPropertyWrite(final String propertyKey) {
        for (int attempts = 0; attempts < 10; attempts++) {
            final Long schema = this.vertexProperties.get(propertyKey);
            if (schema == null) {
                updateVertexProperties(propertyKey);
            } else {
                return schema;
            }
        }
        // This should never happen.
        throw new IllegalStateException("Schema Long value did not exist for Vertex property. Please contact support.");
    }

    public Long getVertexPropertyRead(final String propertyKey) {
        // Reads need to be fully synchronized because can't rely on null to indicate concurrency issue
        synchronized (this.vertexPropertiesKey) {
            if (this.vertexProperties.containsKey(propertyKey)) {
                return this.vertexProperties.get(propertyKey);
            } else {
                if (!missingVertexProperties.get().contains(propertyKey)) {
                    updateVertexProperties(null);
                    missingVertexProperties.get().add(propertyKey);
                }
            }
            final Long schemaValue = this.vertexProperties.get(propertyKey);
            return schemaValue == null ? DUMMY_SCHEMA_LONG : schemaValue;
        }
    }

    public String getVertexPropertyString(final Long storagePropertyKey) {
        for (int attempts = 0; attempts < 10; attempts++) {
            final String propertyKey = this.vertexProperties.inverse().get(storagePropertyKey);
            if (propertyKey == null) {
                updateVertexProperties(null);
            } else {
                return propertyKey;
            }
        }
        // This should never happen.
        throw new IllegalStateException("Schema String value did not exist for Vertex property storage type. Please contact support.");
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
        if (vpSchemaMap != null) {
            for (final Map.Entry<Long, ?> entry : vpSchemaMap.entrySet()) {
                final String key = getVertexPropertyString(entry.getKey());
                if (entry.getValue() == null || key == null || outMap == null) {
                    System.out.println(entry.getKey());
                }
                outMap.put(key, entry.getValue());
            }
        }
    }

    public Long getVpPropertyWrite(final String propertyKey) {
        for (int attempts = 0; attempts < 10; attempts++) {
            final Long schema = this.vpProperties.get(propertyKey);
            if (schema == null) {
                updateVpProperties(propertyKey);
            } else {
                return schema;
            }
        }
        // This should never happen.
        throw new IllegalStateException("Schema Long value did not exist for VP property. Please contact support.");
    }

    public Long getVpPropertyRead(final String propertyKey) {
        // Reads need to be fully synchronized because can't rely on null to indicate concurrency issue
        synchronized (this.vpPropertiesKey) {
            if (this.vpProperties.containsKey(propertyKey)) {
                return this.vpProperties.get(propertyKey);
            } else {
                if (!missingVpProperties.get().contains(propertyKey)) {
                    updateVpProperties(null);
                    missingVpProperties.get().add(propertyKey);
                }
            }
            final Long schemaValue = this.vpProperties.get(propertyKey);
            return schemaValue == null ? DUMMY_SCHEMA_LONG : schemaValue;
        }
    }

    public String getVpPropertyString(final Long storagePropertyKey) {
        for (int attempts = 0; attempts < 10; attempts++) {
            final String propertyKey = this.vpProperties.inverse().get(storagePropertyKey);
            if (propertyKey == null) {
                updateVpProperties(null);
            } else {
                return propertyKey;
            }
        }
        // This should never happen.
        throw new IllegalStateException("Schema String value did not exist for VP property storage type. Please contact support.");
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
        if (vertexVpPropertyMap != null) {
            for (final Map.Entry<Object, Map<Long, Object>> entry : vertexVpPropertyMap.entrySet()) {
                final Map<String, Object> vpPropertyStringMap = new TreeMap<>();
                populateVpPropertySchemaMapToStringMap(entry.getValue(), vpPropertyStringMap);
                outMap.put(entry.getKey(), vpPropertyStringMap);
            }
        }
    }

    public Long getEdgeLabelWrite(final String label) {
        for (int attempts = 0; attempts < 10; attempts++) {
            final Long schema = this.edgeLabels.get(label);
            if (schema == null) {
                updateEdgeLabels(label);
            } else {
                return schema;
            }
        }
        // This should never happen.
        throw new IllegalStateException("Schema Long value did not exist for Edge label. Please contact support.");
    }

    public Long getEdgeLabelRead(final String label) {
        // Reads need to be fully synchronized because can't rely on null to indicate concurrency issue
        synchronized (this.edgeLabelsKey) {
            if (this.edgeLabels.containsKey(label)) {
                return this.edgeLabels.get(label);
            } else {
                if (!missingEdgeLabels.get().contains(label)) {
                    updateEdgeLabels(null);
                    missingEdgeLabels.get().add(label);
                }
            }
            final Long schemaValue = this.edgeLabels.get(label);
            return schemaValue == null ? DUMMY_SCHEMA_LONG : schemaValue;
        }
    }

    public String getEdgeLabelString(final Long storageLabel) {
        for (int attempts = 0; attempts < 10; attempts++) {
            final String label =  this.edgeLabels.inverse().get(storageLabel);
            if (label == null) {
                updateEdgeLabels(null);
            } else {
                return label;
            }
        }
        // This should never happen.
        throw new IllegalStateException("Schema String value did not exist for Edge label storage type. Please contact support.");
    }

    public Long getEdgePropertyWrite(final String propertyKey) {
        for (int attempts = 0; attempts < 10; attempts++) {
            final Long schema = this.edgeProperties.get(propertyKey);
            if (schema == null) {
                updateEdgeProperties(propertyKey);
            } else {
                return schema;
            }
        }
        // This should never happen.
        throw new IllegalStateException("Schema Long value did not exist for Edge property. Please contact support.");
    }

    public Long getEdgePropertyRead(final String propertyKey) {
        // Reads need to be fully synchronized because can't rely on null to indicate concurrency issue
        synchronized (this.edgePropertiesKey) {
            if (this.edgeProperties.containsKey(propertyKey)) {
                return this.edgeProperties.get(propertyKey);
            } else {
                if (!missingEdgeProperties.get().contains(propertyKey)) {
                    updateEdgeProperties(null);
                    missingEdgeProperties.get().add(propertyKey);
                }
            }
            final Long schemaValue = this.edgeProperties.get(propertyKey);
            return schemaValue == null ? DUMMY_SCHEMA_LONG : schemaValue;
        }
    }

    public String getEdgePropertyString(final Long storagePropertyKey) {
        for (int attempts = 0; attempts < 10; attempts++) {
            final String propertyKey =  this.edgeProperties.inverse().get(storagePropertyKey);
            if (propertyKey == null) {
                updateEdgeProperties(null);
            } else {
                return propertyKey;
            }
        }
        // This should never happen.
        throw new IllegalStateException("Schema String value did not exist for Edge property storage type. Please contact support.");
    }

    public void populateEdgePropertyStringMapToSchemaMap(final Map<String, ?> edgePropertyStringMap,
                                                         final Map<Long, Object> outMap) {
        for (final Map.Entry<String, ?> entry : edgePropertyStringMap.entrySet()) {
            final Long schemaKey = getEdgePropertyWrite(entry.getKey());
            outMap.put(schemaKey, entry.getValue());
        }
    }

    public void populateEdgePropertySchemaMapToStringMap(final Map<Long, ?> edgePropertySchemaMap,
                                                         final Map<String, Object> outMap) {
        if (edgePropertySchemaMap != null) {
            for (final Map.Entry<Long, ?> entry : edgePropertySchemaMap.entrySet()) {
                final String key = getEdgePropertyString(entry.getKey());
                outMap.put(key, entry.getValue());
            }
        }
    }

    public void updateAll() {
        updateVertexLabels(null);
        updateVertexProperties(null);
        updateVpProperties(null);
        updateEdgeLabels(null);
        updateEdgeProperties(null);
    }

    public void resetThreadLocals() {
        this.missingVertexProperties.get().clear();
        this.missingVertexLabels.get().clear();
        this.missingVpProperties.get().clear();
        this.missingEdgeLabels.get().clear();
        this.missingEdgeProperties.get().clear();
    }

    public void clearAll() {
        this.vertexLabels.clear();
        this.vertexProperties.clear();
        this.vpProperties.clear();
        this.edgeLabels.clear();
        this.edgeProperties.clear();
    }

    private void updateVertexLabels(final String label) {
        synchronized (this.vertexLabelsKey) {
            if (label == null || !this.vertexLabels.containsKey(label)) {
                updateSchemaMap(label, this.vertexLabelsKey, this.vertexLabels);
            }
        }
    }

    private void updateVertexProperties(final String propertyKey) {
        synchronized (this.vertexPropertiesKey) {
            if (propertyKey == null || !this.vertexProperties.containsKey(propertyKey)) {
                updateSchemaMap(propertyKey, this.vertexPropertiesKey, this.vertexProperties);
            }
        }
    }

    private void updateVpProperties(final String propertyKey) {
        synchronized (this.vpPropertiesKey) {
            if (propertyKey == null || !this.vpProperties.containsKey(propertyKey)) {
                updateSchemaMap(propertyKey, this.vpPropertiesKey, this.vpProperties);
            }
        }
    }

    private void updateEdgeLabels(final String label) {
        synchronized (this.edgeLabelsKey) {
            if (label == null || !this.edgeLabels.containsKey(label)) {
                updateSchemaMap(label, this.edgeLabelsKey, this.edgeLabels);
            }
        }
    }

    private void updateEdgeProperties(final String propertyKey) {
        synchronized (this.edgePropertiesKey) {
            if (propertyKey == null || !this.edgeProperties.containsKey(propertyKey)) {
                updateSchemaMap(propertyKey, this.edgePropertiesKey, this.edgeProperties);
            }
        }
    }

    private void updateSchemaMap(final String schemaKey, final Key recordKey, final BiMap<String, Long> schemaMap) {
        initializeSchemaSet(recordKey);
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

        try {
            final Record record = this.db.writeOperate(null, recordKey, ELEMENT_NOT_FOUND_CODE, true, operations.toArray(new Operation[0]));
            if (record == null) {
                // Need to handle re-initialization of the schema sets if someone drops the entire database.
                this.initializedMap.get(recordKey).set(false);
                initializeSchemaSet(recordKey);
                updateSchemaMap(schemaKey, recordKey, schemaMap);
                return;
            }
            final Object schemaResult = record.getValue(this.db.SCHEMA_BIN);
            final Map<String, Long> latestSchema;
            if (schemaResult instanceof Map) {
                // This was a read-only operation
                latestSchema = (Map<String, Long>) schemaResult;
            } else if (schemaResult instanceof List) {
                // Checked or created a new schema pair
                latestSchema = (Map<String, Long>) ((List<?>) schemaResult).get(1);
            } else {
                // This should never happen
                throw new IllegalStateException("Schema mapping operation returned a type that is not Map or List. Please contact support.");
            }
            schemaMap.clear();
            if (recordKey.equals(this.edgePropertiesKey)) {
                schemaMap.put(EDGE_SUPERNODE_LABEL_KEY, -32L);
                schemaMap.put(EDGE_SUPERNODE_ADJACENT_ID_KEY, -31L);
            }
            schemaMap.putAll(latestSchema);
        } catch (final AerospikeGraphElementNotFoundException e) {
            // Need to handle re-initialization of the schema sets if someone drops the entire database.
            this.initializedMap.get(recordKey).set(false);
            initializeSchemaSet(recordKey);
            updateSchemaMap(schemaKey, recordKey, schemaMap);
        }
    }

    private void initializeSchemaSet(final Key recordKey) {
        if (!this.initializedMap.get(recordKey).getAndSet(true)) {
            final WritePolicy policy = new WritePolicy();
            policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;

            try {
                final int initialSchemaValue;
                // Only Vertex label values aren't stored in a CDT in Aerospike and therefore doesn't use MessagePack.
                if (recordKey.equals(this.vertexLabelsKey)) {
                    initialSchemaValue = 0;
                } else if (recordKey.equals(this.edgePropertiesKey)) {
                    // -32 is for label; -31 is for adjacent ID key
                    initialSchemaValue = -30;
                } else {
                    initialSchemaValue = -32;
                }
                final Map<String, Long> initialMap = new HashMap<>();
                final Operation initializeSchemaValue = Operation.put(new Bin(this.db.COUNTER_BIN, initialSchemaValue));
                final Operation initializeSchemaMap = Operation.put(new Bin(this.db.SCHEMA_BIN, initialMap));
                LOG.info("Initializing {} schema data.", this.readableNames.get(recordKey));
                this.db.writeOperate(policy, recordKey, KEY_EXISTS_CODE, true, initializeSchemaMap, initializeSchemaValue);
            } catch (final AerospikeGraphException e) {
                if (e.errorCode == ResultCode.KEY_EXISTS_ERROR) {
                    LOG.info("Existing {} schema data found.", this.readableNames.get(recordKey));
                } else {
                    this.initializedMap.get(recordKey).set(false);
                    throw e;
                }
            }
        }
    }
}
