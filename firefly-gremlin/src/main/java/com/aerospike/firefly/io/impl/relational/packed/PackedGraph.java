package com.aerospike.firefly.io.impl.relational.packed;

import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.RelationalGraph;
import com.aerospike.firefly.io.impl.relational.RelationalVertex;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyGraphStepStrategy;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;

import static com.aerospike.firefly.util.ConfigurationHelper.Keys.VERTEX_PROPERTY_NAME_TO_VALUE;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
final public class PackedGraph extends RelationalGraph {
    private static final Logger LOG = LoggerFactory.getLogger(PackedGraph.class);
    public static final String DATA_MODEL = "packed";

    /**
     * Constructor for PackedGraph.
     *
     * @param db   AerospikeConnection.
     * @param conf Configuration.
     */
    public PackedGraph(final AerospikeConnection db, final Configuration conf) {
        super(db, conf);
    }

    static {
        TraversalStrategies.GlobalCache.registerStrategies(
                PackedGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone()
                        .addStrategies(FireflyGraphStepStrategy.instance()));
    }

    @Override
    protected int getTypeHint() {
        return PackedVertex.VERTEX_TYPE_HINT;
    }

    @Override
    protected String getDataModel() {
        return DATA_MODEL;
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
        return PackedVertex.fromRecord(this, keyRecord);
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
    public <V> FireflyVertexProperty<V> writeVertexProperty(final FireflyId idValue,
                                                            final FireflyVertex vertex,
                                                            final String key,
                                                            final V value) {
        // Write vertex property to Aerospike.
        final FireflyVertexProperty<V> fireflyVertexProperty = PackedVertexProperty.writeVertexProperty(this, vertex, idValue, key, value);

        // Append vertex property to vertex.
        vertex.writeVertexProperty(fireflyVertexProperty);

        // Return FireflyVertexProperty.
        return fireflyVertexProperty;
    }

    /**
     * Create vertex property from a FireflyRecord and parent vertex id.
     *
     * @param fireflyRecord FireflyRecord.
     * @param id            Parent vertex id.
     * @param key           Key of property for record.
     * @param <V>           Type of FireflyVertexProperty.
     * @return FireflyVertexProperty.
     */
    private <V> FireflyVertexProperty<V> vertexPropertyFromRecord(final FireflyRecord fireflyRecord, final String key, final FireflyId id) {
        return PackedVertexProperty.fromRecord(this, key, fireflyRecord, id);
    }

    /**
     * Lookup VertexProperty with a particular Value by index
     *
     * @param key   Property Key
     * @param value Property Value being searched for
     * @return Iterator of VertexProperty results
     */
    @Override
    public Iterator<FireflyVertexProperty> queryVertexPropertyStringIndex(final String key, final Object value) {
        final Iterator<KeyRecord> rsi =
                db.queryIndex(
                        db.getElementPropertySet(FireflyVertex.class),
                        db.STRING_V_VP_KV_INDEX,
                        Filter.contains(db.VERTEX_PROPERTY_NAME_TO_VALUE, IndexCollectionType.MAPVALUES, (String) value));
        final Iterator<FireflyVertexProperty> vps = IteratorUtils.map(rsi, kr ->
                vertexPropertyFromRecord(FireflyRecord.fromRecord(db, kr.key, kr.record), key,
                        FireflyId.of(FireflyVertex.class, kr.key.userKey.getObject())));
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
                filter = Filter.contains(db.VERTEX_PROPERTY_NAME_TO_VALUE, IndexCollectionType.MAPVALUES, (Long.valueOf((Integer) value)));
            else if (Long.class.isAssignableFrom(value.getClass()))
                filter = Filter.contains(db.VERTEX_PROPERTY_NAME_TO_VALUE, IndexCollectionType.MAPVALUES, (Long) value);
            else
                throw new RuntimeException(String.format("%s not a supported numeric match type", value.getClass()));
        } else {
            throw new RuntimeException(String.format("%s not assignable to Number", value.getClass()));
        }
        final Iterator<KeyRecord> rsi = db.queryIndex(db.VERTEX_AERO_SET, db.NUMERIC_V_VP_KV_INDEX, filter);

        final Iterator<FireflyVertexProperty> vps = IteratorUtils.map(rsi, kr ->
                vertexPropertyFromRecord(FireflyRecord.fromRecord(db, kr.key, kr.record), key,
                        FireflyId.of(FireflyVertex.class, kr.key.userKey.getObject())));
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
                filter = Filter.range(db.VERTEX_PROPERTY_NAME_TO_VALUE, IndexCollectionType.MAPVALUES, Long.MIN_VALUE, val);
            else if (predicate.getBiPredicate().equals(Compare.gt))
                filter = Filter.range(db.VERTEX_PROPERTY_NAME_TO_VALUE, IndexCollectionType.MAPVALUES, val, Long.MAX_VALUE);
            else throw new RuntimeException(String.format("%s not a supported predicate", predicate));
        } else {
            throw new RuntimeException(String.format("%s not a supported numeric type", predicate.getValue().getClass()));
        }

        final Iterator<KeyRecord> rsi = db.queryIndex(db.VERTEX_AERO_SET, db.NUMERIC_V_VP_KV_INDEX, filter);
        final Iterator<FireflyVertexProperty> vps = IteratorUtils.map(rsi, kr ->
                vertexPropertyFromRecord(FireflyRecord.fromRecord(db, kr.key, kr.record), key,
                        FireflyId.of(FireflyVertex.class, kr.key.userKey.getObject())));
        return IteratorUtils.filter(vps, vp -> vp.key().equals(key));
    }
}
