package com.aerospike.firefly.io.impl.linked;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.async.Throttles;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.ScanRecordSequenceListener;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.NumericIdManager;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.util.ConfigurationHelper.Keys.KEY_VALUE;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
final public class LinkedGraph extends FireflyGraph {
    private static final Logger LOG = LoggerFactory.getLogger(LinkedGraph.class);
    public static final String DATA_MODEL = "linked";

    public LinkedGraph(AerospikeConnection db, final Configuration conf) {
        super(db, conf);
    }

    /**
     * Read the Record of properties associated with the Element from PROPERTY_AERO_SET
     * remove k from the ELEMENT_PROPERTIES map
     *
     * @param element Element to remove property from
     * @param key     property key to remove
     */
    @Override
    public void removeProperty(final FireflyElement element, final String key) {
        db.removeTypeHintedValueFromMap(
                db.getElementPropertySet(element.getClass()),
                FireflyId.fromElement(element),
                db.getElementPropertySet(element.getClass()), key);
    }

    @Override
    public FireflyVertex writeVertex(FireflyId idValue, String label, List<Map.Entry<String, Object>> properties) {
        return LinkedVertex.writeVertex(this, idValue, label, properties);
    }

    @Override
    public FireflyVertex readVertex(final FireflyId idValue) {
        return LinkedVertex.readVertex(this, idValue);
    }

    @Override
    public FireflyEdge writeEdge(final FireflyId edgeId,
                                 final String label,
                                 final List<Map.Entry<String, Object>> properties,
                                 final FireflyVertex inVertex,
                                 final FireflyVertex outVertex) {
        // Add edge to inVertex and outVertex.
        inVertex.writeEdge(Direction.IN, edgeId, label);
        outVertex.writeEdge(Direction.OUT, edgeId, label);

        // Write edge to Aerospike and return FireflyEdge.
        return LinkedEdge.writeEdge(this, edgeId, label, properties, inVertex, outVertex);
    }

    @Override
    public FireflyEdge readEdge(final FireflyId edgeId) {
        return LinkedEdge.readEdge(this, edgeId);
    }

    /**
     * Write vertex property to Aerospike.
     *
     * @param idValue FireflyId of vertex property to write.
     * @param vertex  Vertex to write property to.
     * @param key     Key of property to write.
     * @param value   Value of property to write.
     * @param <V>     Type of value to write.
     * @return FireflyVertexProperty
     */
    @Override
    public <V> FireflyVertexProperty<V> writeVertexProperty(final FireflyId idValue, final FireflyVertex vertex, final String key, final V value) {
        // Write vertex property to Aerospike.
        final FireflyVertexProperty<V> fireflyVertexProperty = LinkedVertexProperty.writeVertexProperty(this, vertex, idValue, key, value);

        // Append vertex property to vertex.
        vertex.writeVertexProperty(fireflyVertexProperty);

        // Return FireflyVertexProperty.
        return fireflyVertexProperty;
    }

    /**
     * Determine if a vertex exists.
     *
     * @param idValue vertex id to check.
     * @return true if vertex exists, false otherwise.
     */
    @Override
    public boolean vertexExists(final FireflyId idValue) {
        LOG.debug("Checking if vertex {} exists.", idValue.value());
        final Key key = FireflyRecord.getKey(db.getNamespace(), db.VERTEX_AERO_SET, idValue.toNumericId());
        return db.exists(key);
    }

    /**
     * Determine if an edge exists.
     *
     * @param idValue edge id to check.
     * @return true if edge exists, false otherwise.
     */
    @Override
    public boolean edgeExists(final FireflyId idValue) {
        LOG.debug("Checking if edge {} exists.", idValue.value());
        final Key key = FireflyRecord.getKey(db.getNamespace(), db.EDGE_AERO_SET, idValue.toNumericId());
        return db.exists(key);
    }

    /**
     * Determine if a vertex property exists.
     *
     * @param vpId vertex property id to check.
     * @return true if vertex property exists, false otherwise.
     */
    @Override
    public boolean vertexPropertyExists(final FireflyId vpId) {
        LOG.debug("Checking if vertex property {} exists.", vpId.value());
        final Key key = FireflyRecord.getKey(db.getNamespace(), db.VERTEX_AERO_SET, vpId.toNumericId());
        return db.exists(key);
    }

    /**
     * Return a Graph variable value by name
     *
     * @param key Graph variable key
     * @param <V> type
     * @return Graph variable value
     */
    @Override
    public <V> V readGraphVariable(final String key) {
        return db.readTypeHintedValueFromMap(db.GRAPH_VARIABLES_SET, FireflyId.of(null, db.GRAPH_VARIABLES_RECORD), db.GRAPH_VARIABLES_MAP, key);
    }

    /**
     * Return a Set of all the Graph variable names
     *
     * @return Set of Graph variable keys
     */
    @Override
    public Set<String> readGraphVariableKeys() {
        final FireflyRecord fireflyRecord = FireflyRecord.read(db, db.GRAPH_VARIABLES_SET, FireflyId.of(null, db.GRAPH_VARIABLES_RECORD));
        if (fireflyRecord == null) return new HashSet<>();
        final Map<String, ?> m = (Map<String, ?>) fireflyRecord.record.getMap(db.GRAPH_VARIABLES_MAP);
        return m.keySet();
    }

    /**
     * Write a Graph variable
     *
     * @param key   Graph variable key
     * @param value Graph variable value to write
     * @param <V>   Graph variable value type
     */
    @Override
    public <V> void writeGraphVariable(final String key, final V value) {
        db.writeTypeHintedValueToMap(db.GRAPH_VARIABLES_SET, FireflyId.of(null, db.GRAPH_VARIABLES_RECORD), db.GRAPH_VARIABLES_MAP, key, value);
    }

    /**
     * Remove a Graph variable
     *
     * @param key Graph variable key to remove
     */
    @Override
    public void removeGraphVariable(final String key) {
        db.removeTypeHintedValueFromMap(db.GRAPH_VARIABLES_SET, FireflyId.of(null, db.GRAPH_VARIABLES_RECORD), db.GRAPH_VARIABLES_MAP, key);
    }


    /**
     * Lookup Edges with a particular property value by index
     *
     * @param key   Property Key
     * @param value Property Value being searched for
     * @return an Iterator of Edges
     */
    @Override
    public Iterator<FireflyEdge> queryEdgePropertyStringMatchIndex(String key, Object value) {
        if (!String.class.isAssignableFrom(value.getClass())) {
            throw new RuntimeException(String.format("%s not a string", value.getClass()));
        }
        final Iterator<KeyRecord> rsi = db.queryIndex(db.EDGE_AERO_SET, db.STRING_E_KV_INDEX,
                Filter.contains(db.getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, (String) value));
        final Iterator<FireflyEdge> edges = IteratorUtils.map(rsi, kr ->
                readEdge(FireflyId.of(FireflyEdge.class, kr.key.userKey.getObject())));
        return IteratorUtils.filter(edges, edge -> edge.property(key).value().equals(value));
    }

    /**
     * Lookup Edge by numeric match on property value
     *
     * @param key       Property key to match
     * @param predicate type of match
     * @return Iterator of FireflyEdge results
     */
    @Override
    public Iterator<FireflyEdge> queryEdgePropertyNumericMatchIndex(String key, P<?> predicate) {
        final Object value = predicate.getValue();
        Filter filter;
        if (Number.class.isAssignableFrom(value.getClass())) {
            if (Integer.class.isAssignableFrom(value.getClass()))
                filter = Filter.contains(db.getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, (Long.valueOf((Integer) value)));
            else if (Long.class.isAssignableFrom(value.getClass()))
                filter = Filter.contains(db.getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, (Long) value);
            else
                throw new RuntimeException(String.format("%s not a supported numeric match type", value.getClass()));
        } else {
            throw new RuntimeException(String.format("%s not assignable to Number", value.getClass()));
        }

        final Iterator<KeyRecord> rsi = db.queryIndex(db.EDGE_AERO_SET, db.NUMERIC_E_KV_INDEX, filter);
        final Iterator<FireflyEdge> edges = IteratorUtils.map(rsi, kr ->
                readEdge(FireflyId.of(FireflyEdge.class, kr.key.userKey.getObject())));
        return IteratorUtils.filter(edges, edge -> edge.properties(key).hasNext());
    }

    /**
     * Lookup Edge by numeric range match on property value
     *
     * @param key       Property key to match
     * @param predicate type of match (lt or gt) with value embedded
     * @return Iterator of FireflyEdge results
     */
    @Override
    public Iterator<FireflyEdge> queryEdgePropertyNumericRangeIndex(String key, P<?> predicate) {
        Filter filter;
        if (Number.class.isAssignableFrom(predicate.getValue().getClass())) {
            final long val = Long.class.isAssignableFrom(predicate.getValue().getClass()) ?
                    (long) predicate.getValue() : Long.valueOf((Integer) predicate.getValue());
            if (predicate.getBiPredicate().equals(Compare.lt))
                filter = Filter.range(db.getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, Long.MIN_VALUE, val);
            else if (predicate.getBiPredicate().equals(Compare.gt))
                filter = Filter.range(db.getElementPropertySet(FireflyEdge.class), IndexCollectionType.MAPVALUES, val, Long.MAX_VALUE);
            else throw new RuntimeException(String.format("%s not a supported predicate", predicate));
        } else {
            throw new RuntimeException(String.format("%s not a supported numeric type", predicate.getValue().getClass()));
        }
        final Iterator<KeyRecord> rsi = db.queryIndex(db.EDGE_AERO_SET, db.NUMERIC_E_KV_INDEX, filter);
        final Iterator<FireflyEdge> edges = IteratorUtils.map(rsi, kr ->
                readEdge(FireflyId.of(FireflyEdge.class, kr.key.userKey.getObject())));
        return IteratorUtils.filter(edges, edge -> edge.properties(key).hasNext());
    }

    /**
     * Lookup Vertex by string match on property value
     *
     * @param value value to match
     * @return Iterator of FireflyVertex results
     */
    @Override
    public Iterator<FireflyVertex> queryVertexLabelStringIndex(Object value) {
        final Iterator<KeyRecord> iter = db.queryIndex(db.VERTEX_AERO_SET, db.V_LABEL_INDEX,
                Filter.contains(AerospikeConnection.LABEL, IndexCollectionType.DEFAULT, (String) value));
        return IteratorUtils.map(iter, kr ->
                readVertex(FireflyId.of(FireflyVertex.class, kr.key.userKey.getObject())));
    }

    /**
     * Lookup Edge by string match on label value
     *
     * @param value label value to match
     * @return Iterator of FireflyEdge results
     */
    @Override
    public Iterator<FireflyEdge> queryEdgeLabelStringIndex(Object value) {
        final Iterator<KeyRecord> iter = db.queryIndex(db.getElementPropertySet(FireflyEdge.class), db.E_LABEL_INDEX,
                Filter.contains(AerospikeConnection.LABEL, IndexCollectionType.DEFAULT, (String) value));
        return IteratorUtils.map(iter, kr ->
                readEdge(FireflyId.of(FireflyEdge.class, kr.key.userKey.getObject())));
    }

    /**
     * Lookup VertexProperty with a particular Value by index
     *
     * @param key   Property Key
     * @param value Property Value being searched for
     * @return Iterator of VertexProperty results
     */
    @Override
    public Iterator<FireflyVertexProperty> queryVertexPropertyStringIndex(String key, Object value) {
        final Iterator<KeyRecord> rsi =
                db.queryIndex(
                        db.VERTEX_PROPERTY_AERO_SET,
                        db.STRING_VP_KV_INDEX,
                        Filter.contains(KEY_VALUE, IndexCollectionType.MAPVALUES, (String) value));
        final Iterator<FireflyVertexProperty> vps = IteratorUtils.map(rsi, kr ->
                LinkedVertexProperty.fromRecord(this, FireflyRecord.fromRecord(db, kr.key, kr.record),
                        FireflyId.of(FireflyVertex.class, kr.record.getLong(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.PARENT_VERTEX_ID, db.conf)))));
        return IteratorUtils.filter(vps, vp -> vp.key().equals(key));
    }

    /**
     * Lookup VertexProperty by numeric range match on property value
     *
     * @param key       Key to match
     * @param predicate Match Predicate with value embedded
     * @return Iterator of FireflyVertexProperty results
     */
    @Override
    public Iterator<FireflyVertexProperty> queryVertexPropertyNumberMatchIndex(String key, P<?> predicate) {
        final Object value = predicate.getValue();
        Filter filter;
        if (Number.class.isAssignableFrom(value.getClass())) {
            if (Integer.class.isAssignableFrom(value.getClass()))
                filter = Filter.contains(KEY_VALUE, IndexCollectionType.MAPVALUES, (Long.valueOf((Integer) value)));
            else if (Long.class.isAssignableFrom(value.getClass()))
                filter = Filter.contains(KEY_VALUE, IndexCollectionType.MAPVALUES, (Long) value);
            else
                throw new RuntimeException(String.format("%s not a supported numeric match type", value.getClass()));
        } else {
            throw new RuntimeException(String.format("%s not assignable to Number", value.getClass()));
        }
        final Iterator<KeyRecord> rsi = db.queryIndex(db.VERTEX_PROPERTY_AERO_SET, db.NUMERIC_VP_KV_INDEX, filter);

        final Iterator<FireflyVertexProperty> vps = IteratorUtils.map(rsi, kr ->
                LinkedVertexProperty.fromRecord(this, FireflyRecord.fromRecord(db, kr.key, kr.record),
                        FireflyId.of(FireflyVertex.class, kr.record.getLong(
                                ConfigurationHelper.getOrDefault(
                                        ConfigurationHelper.Keys.PARENT_VERTEX_ID, db.conf)))));
        return IteratorUtils.filter(vps, vp -> vp.key().equals(key));
    }

    /**
     * Lookup VertexProperty by numeric range match on property value
     *
     * @param key       Key to match
     * @param predicate type of match (lt or gt) with value embedded
     * @return Iterator of FireflyVertexProperty results
     */
    @Override
    public Iterator<FireflyVertexProperty> queryVertexPropertyNumberRangeIndex(String key, P<?> predicate) {
        Filter filter;
        if (Number.class.isAssignableFrom(predicate.getValue().getClass())) {
            final long val = Long.class.isAssignableFrom(predicate.getValue().getClass()) ?
                    (long) predicate.getValue() : Long.valueOf((Integer) predicate.getValue());
            if (predicate.getBiPredicate().equals(Compare.lt))
                filter = Filter.range(KEY_VALUE, IndexCollectionType.MAPVALUES, Long.MIN_VALUE, val);
            else if (predicate.getBiPredicate().equals(Compare.gt))
                filter = Filter.range(KEY_VALUE, IndexCollectionType.MAPVALUES, val, Long.MAX_VALUE);
            else throw new RuntimeException(String.format("%s not a supported predicate", predicate));
        } else {
            throw new RuntimeException(String.format("%s not a supported numeric type", predicate.getValue().getClass()));
        }

        final Iterator<KeyRecord> rsi = db.queryIndex(db.VERTEX_PROPERTY_AERO_SET, db.NUMERIC_VP_KV_INDEX, filter);
        final Iterator<FireflyVertexProperty> vps = IteratorUtils.map(rsi, kr ->
                LinkedVertexProperty.fromRecord(this, FireflyRecord.fromRecord(db, kr.key, kr.record),
                        FireflyId.of(FireflyVertex.class, kr.record.getLong(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.PARENT_VERTEX_ID, db.conf)))));
        return IteratorUtils.filter(vps, vp -> vp.key().equals(key));
    }

    protected Iterator<Long> scanAllVertices() {
        //@todo performance
        LOG.trace("Scanning {} ids.", db.VERTEX_AERO_SET);
        final Iterator<Map.Entry<Key, Record>> i = db.scanAllKeysInSet(db.VERTEX_AERO_SET, null);
        return IteratorUtils.map(i, keyRecordEntry -> NumericIdManager.convert(keyRecordEntry.getKey().userKey.getObject()));
    }

    @Override
    public long getVertexCount() {
        return 0;
    }

    @Override
    public long getEdgeCount() {
        return 0;
    }
}
