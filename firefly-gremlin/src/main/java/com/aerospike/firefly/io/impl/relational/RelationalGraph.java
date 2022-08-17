package com.aerospike.firefly.io.impl.relational;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.NumericIdManager;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public abstract class RelationalGraph extends FireflyGraph {
    private static final Logger LOG = LoggerFactory.getLogger(RelationalGraph.class);

    /**
     * Constructor for RelationalGraph.
     *
     * @param db   AerospikeConnection.
     * @param conf Configuration.
     */
    public RelationalGraph(AerospikeConnection db, final Configuration conf) {
        super(db, conf);
    }

    /**
     * Function to write edge to Aerospike.
     *
     * @param edgeId     Edge id.
     * @param label      Edge label.
     * @param properties Edge properties.
     * @param inVertex   In vertex of edge.
     * @param outVertex  Out vertex of edge.
     * @return Edge.
     */
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
        return RelationalEdge.writeEdge(this, edgeId, label, properties, inVertex, outVertex);
    }

    protected abstract int getTypeHint();

    /**
     * Function to write vertex to Aerospike.
     *
     * @param idValue    Id of vertex.
     * @param label      Label of vertex.
     * @param properties List of vertex properties.
     * @return Vertex.
     */
    @Override
    public FireflyVertex writeVertex(final FireflyId idValue,
                                     final String label,
                                     final List<Map.Entry<String, Object>> properties) {
        return RelationalVertex.writeVertex(this, idValue, label, properties, getTypeHint());
    }

    /**
     * Function to read edge from Aerospike.
     *
     * @param edgeId Edge id.
     * @return Edge.
     */
    @Override
    public FireflyEdge readEdge(final FireflyId edgeId) {
        return RelationalEdge.readEdge(this, edgeId);
    }

    /**
     * Function to create edge from a record.
     *
     * @param keyRecord Record to use.
     * @return Edge.
     */
    @Override
    public FireflyEdge edgeFromRecord(final KeyRecord keyRecord) {
        return RelationalEdge.fromRecord(this, keyRecord);
    }

    /**
     * Function to read vertex from Aerospike.
     *
     * @param idValue Id of vertex.
     * @return Vertex.
     */
    @Override
    public FireflyVertex readVertex(final FireflyId idValue) {
        return RelationalVertex.readVertex(this, idValue);
    }

    /**
     * Function to create vertex from a record.
     *
     * @param keyRecord Record to use.
     * @return Vertex.
     */
    @Override
    public FireflyVertex vertexFromRecord(final KeyRecord keyRecord) {
        return RelationalVertex.fromRecord(this, keyRecord);
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
        return IteratorUtils.map(iter, this::vertexFromRecord);
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
        return IteratorUtils.map(iter, this::edgeFromRecord);
    }

    protected Iterator<Long> scanAllVertices() {
        //@todo performance
        LOG.trace("Scanning {} ids.", db.VERTEX_AERO_SET);
        final Iterator<Map.Entry<Key, Record>> i = db.scanAllKeysInSet(db.VERTEX_AERO_SET, null);
        return IteratorUtils.map(i, keyRecordEntry -> NumericIdManager.convert(keyRecordEntry.getKey().userKey.getObject()));
    }

    @Override
    public <V> Property<V> writeProperty(final FireflyElement element, final String key, final V value) {
        return RelationalProperty.writeProperty(this, element, key, value);
    }

    @Override
    public <V> Map<String, Property<V>> readProperties(final FireflyElement element) {
        return RelationalProperty.readProperties(this, element);
    }

    @Override
    public <V> Property<V> readProperty(final FireflyElement element, final String key) {
        return RelationalProperty.readProperty(this, element, key);
    }

    @Override
    public long getVertexCount() {
        return AerospikeConnection.InfoOps.getSetSize(db.VERTEX_AERO_SET, db.getNamespace(), db.getClient());
    }

    @Override
    public long getEdgeCount() {
        return AerospikeConnection.InfoOps.getSetSize(db.EDGE_AERO_SET, db.getNamespace(), db.getClient());
    }
}
