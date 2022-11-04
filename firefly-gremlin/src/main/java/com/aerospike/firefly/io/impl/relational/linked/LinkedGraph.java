package com.aerospike.firefly.io.impl.relational.linked;

import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.RelationalGraph;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyGraphStepStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyTraversalCacheStrategy;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;

import static com.aerospike.firefly.util.ConfigurationHelper.Keys.KEY_VALUE;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
final public class LinkedGraph extends RelationalGraph {
    public static final String DATA_MODEL = "linked";

    static {
        TraversalStrategies.GlobalCache.registerStrategies(
                LinkedGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(FireflyGraph.class).clone()
                        .addStrategies(FireflyGraphStepStrategy.instance())
                        .addStrategies(OptionsStrategy.build().create()));
    }

    /**
     * Constructor for LinkedGraph.
     *
     * @param db   AerospikeConnection.
     * @param conf Configuration.
     */
    public LinkedGraph(final AerospikeConnection db, final Configuration conf) {
        super(db, conf);
        if (Boolean.parseBoolean(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.ENABLE_SUBGRAPH_CACHE_STRATEGY, conf))) {
            TraversalStrategies.GlobalCache.registerStrategies(
                    LinkedGraph.class,
                    TraversalStrategies.GlobalCache.getStrategies(FireflyGraph.class).clone()
                            .addStrategies(FireflyTraversalCacheStrategy.instance()));
        }
    }


    static {
        TraversalStrategies.GlobalCache.registerStrategies(
                LinkedGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone()
                        .addStrategies(FireflyGraphStepStrategy.instance()));
    }

    @Override
    protected int getTypeHint() {
        return LinkedVertex.VERTEX_TYPE_HINT;
    }

    @Override
    public String getDataModel() {
        return getDataModelName();
    }

    public static String getDataModelName() {
        return DATA_MODEL;
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
        // Write vertex property to vertex property record first so if we fail we don't end up with null property inside vertex.

        // Write vertex property to Aerospike.
        final FireflyVertexProperty<V> fireflyVertexProperty = LinkedVertexProperty.writeVertexProperty(this, vertex, idValue, key, value);

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
     * @param <V>           Type of FireflyVertexProperty\.
     * @return FireflyVertexProperty.
     */
    private <V> FireflyVertexProperty<V> vertexPropertyFromRecord(final FireflyRecord fireflyRecord, final FireflyId id) {
        return LinkedVertexProperty.fromRecord(this, fireflyRecord, id);
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
                        db.getElementPropertySet(FireflyVertexProperty.class),
                        db.STRING_VP_KV_INDEX,
                        Filter.contains(KEY_VALUE, IndexCollectionType.MAPVALUES, (String) value));
        final Iterator<FireflyVertexProperty> vps = IteratorUtils.map(rsi, kr ->
                vertexPropertyFromRecord(FireflyRecord.fromRecord(db, kr.key, kr.record),
                        FireflyIdFactory.createId(kr.record.getLong(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.PARENT_VERTEX_ID, db.conf)))));
        return IteratorUtils.filter(vps, vp -> vp != null && vp.key().equals(key));
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
                filter = Filter.contains(db.KEY_VALUE, IndexCollectionType.MAPVALUES, (Long.valueOf((Integer) value)));
            else if (Long.class.isAssignableFrom(value.getClass()))
                filter = Filter.contains(db.KEY_VALUE, IndexCollectionType.MAPVALUES, (Long) value);
            else
                throw new RuntimeException(String.format("%s not a supported numeric match type", value.getClass()));
        } else {
            throw new RuntimeException(String.format("%s not assignable to Number", value.getClass()));
        }
        final Iterator<KeyRecord> rsi = db.queryIndex(db.VERTEX_PROPERTY_AERO_SET, db.NUMERIC_VP_KV_INDEX, filter);

        final Iterator<FireflyVertexProperty> vps = IteratorUtils.map(rsi, kr ->
                vertexPropertyFromRecord(FireflyRecord.fromRecord(db, kr.key, kr.record),
                        FireflyIdFactory.createId(kr.record.getLong(
                                ConfigurationHelper.getOrDefault(
                                        ConfigurationHelper.Keys.PARENT_VERTEX_ID, db.conf)))));
        return IteratorUtils.filter(vps, vp -> vp != null && vp.key().equals(key));
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
                filter = Filter.range(db.KEY_VALUE, IndexCollectionType.MAPVALUES, Long.MIN_VALUE, val);
            else if (predicate.getBiPredicate().equals(Compare.gt))
                filter = Filter.range(db.KEY_VALUE, IndexCollectionType.MAPVALUES, val, Long.MAX_VALUE);
            else throw new RuntimeException(String.format("%s not a supported predicate", predicate));
        } else {
            throw new RuntimeException(String.format("%s not a supported numeric type", predicate.getValue().getClass()));
        }

        final Iterator<KeyRecord> rsi = db.queryIndex(db.VERTEX_PROPERTY_AERO_SET, db.NUMERIC_VP_KV_INDEX, filter);
        final Iterator<FireflyVertexProperty> vps = IteratorUtils.map(rsi, kr ->
                vertexPropertyFromRecord(FireflyRecord.fromRecord(db, kr.key, kr.record),
                        FireflyIdFactory.createId(kr.record.getLong(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.PARENT_VERTEX_ID, db.conf)))));
        return IteratorUtils.filter(vps, vp -> vp != null && vp.key().equals(key));
    }

    @Override
    public void close() {
        super.close();
        TraversalStrategies.GlobalCache
                .getStrategies(LinkedGraph.class)
                .removeStrategies(FireflyTraversalCacheStrategy.class);
    }
}
