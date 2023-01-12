package com.aerospike.firefly.io.impl.relational.packed;

import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.io.impl.relational.RelationalGraph;
import com.aerospike.firefly.io.impl.relational.RelationalProperty;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyReadThroughCacheStrategy;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class PackedGraph extends RelationalGraph {
    public static final String DATA_MODEL = "packed";

    /**
     * Constructor for PackedGraph.
     *
     * @param db   AerospikeConnection.
     * @param conf Configuration.
     */
    public PackedGraph(final AerospikeConnection db, final Configuration conf) {
        super(db, conf);
        synchronized (PackedGraph.class) {
            TraversalStrategies.GlobalCache.registerStrategies(
                    PackedGraph.class,
                    TraversalStrategies.GlobalCache.getStrategies(FireflyGraph.class).clone());
        }
    }

    @Override
    protected int getTypeHint() {
        return PackedVertex.VERTEX_TYPE_HINT;
    }

    @Override
    public String getDataModel() {
        return getDataModelName();
    }

    public static String getDataModelName() {
        return DATA_MODEL;
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
                        FireflyIdFactory.createFromRecord(db, FireflyRecord.fromRecord(db, kr.key, kr.record))));
        return IteratorUtils.filter(vps, vp -> key.equals(vp.key()));
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
                        FireflyIdFactory.createFromRecord(db, FireflyRecord.fromRecord(db, kr.key, kr.record))));
        return IteratorUtils.filter(vps, vp -> key.equals(vp.key()));
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
                        FireflyIdFactory.createFromRecord(db, FireflyRecord.fromRecord(db, kr.key, kr.record))));
        return IteratorUtils.filter(vps, vp -> key.equals(vp.key()));
    }

    /**
     * Write a property to an element in this graph. Contains specific logic to handle packed vertex properties.
     *
     * @param element   The element that the property is applied to.
     * @param key       The property key.
     * @param value     The property value.
     * @return          The newly written property.
     * @param <V>       Value type of the property.
     */
    @Override
    public <V> Property<V> writeProperty(final FireflyElement element, final String key, final V value) {
        if (element instanceof PackedVertexProperty<?>) {
            FireflyHelper.validatePropertyValue(value);
            ((PackedVertexProperty<?>) element).writeProperty(key, value);
            return new RelationalProperty<>(this, element, key, value);
        } else {
            return super.writeProperty(element, key, value);
        }
    }

    /**
     * Read the properties on an element.
     *
     * @param element   The element to read the properties of.
     * @return          The properties on the element.
     * @param <V>       Value type of the property.
     */
    @Override
    public <V> Map<String, Property<V>> readProperties(final FireflyElement element) {
        if (element instanceof PackedVertexProperty<?>) {
            return ((PackedVertexProperty<?>) element).readProperties();
        } else {
            return super.readProperties(element);
        }
    }

    /**
     * Read a property on an element.
     *
     * @param element   The element to read the properties of.
     * @param key       The key of the property to read.
     * @return          The property on the element with the specified key.
     * @param <V>       Value type of the property.
     */
    @Override
    public <V> Property<V> readProperty(final FireflyElement element, final String key) {
        if (element instanceof PackedVertexProperty<?>) {
            return (Property<V>) readProperties(element).get(key);
        } else {
            return super.readProperty(element, key);
        }
    }

    @Override
    public void close() {
        super.close();
        TraversalStrategies.GlobalCache
                .getStrategies(PackedGraph.class)
                .removeStrategies(FireflyReadThroughCacheStrategy.class);
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
        if (element instanceof PackedVertexProperty) {
            ((PackedVertexProperty<?>) element).removeProperty(key);
        } else {
            super.removeProperty(element, key);
        }
    }
}
