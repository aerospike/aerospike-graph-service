package com.aerospike.firefly.io.impl.relational.packed;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.impl.relational.RelationalGraph;
import com.aerospike.firefly.io.impl.relational.RelationalProperty;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Property;

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
     * Write a property to an element in this graph. Contains specific logic to handle packed vertex properties.
     *
     * @param element The element that the property is applied to.
     * @param key     The property key.
     * @param value   The property value.
     * @param <V>     Value type of the property.
     * @return The newly written property.
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
     * @param element The element to read the properties of.
     * @param <V>     Value type of the property.
     * @return The properties on the element.
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
     * @param element The element to read the properties of.
     * @param key     The key of the property to read.
     * @param <V>     Value type of the property.
     * @return The property on the element with the specified key.
     */
    @Override
    public <V> Property<V> readProperty(final FireflyElement element, final String key) {
        if (element instanceof PackedVertexProperty<?>) {
            return (Property<V>) readProperties(element).get(key);
        } else {
            return super.readProperty(element, key);
        }
    }
}
