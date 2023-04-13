package com.aerospike.firefly.io.impl.relational.packed;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.impl.relational.RelationalGraph;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

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

    // This function is used via reflection in Upgrade.java. Removing will cause issues.
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
                                                            final V value,
                                                            final Object... keyValues) {
        final Map<String, Object> properties = new TreeMap<>();
        final Map<String, Long> typeHints = new TreeMap<>();
        final boolean allowNullProperties = features().vertex().properties().supportsNullPropertyValues();

        for (int i = 0; i < keyValues.length; i = i + 2) {
            if (!keyValues[i].equals(T.id) && !keyValues[i].equals(T.label))
                if (keyValues[i + 1] != null) {
                    properties.put((String) keyValues[i], keyValues[i + 1]);
                    typeHints.put((String) keyValues[i], AerospikeConnection.getSupportedType(keyValues[i + 1].getClass()));
                } else if (allowNullProperties) {
                    properties.put((String) keyValues[i], keyValues[i + 1]);
                    typeHints.put((String) keyValues[i], AerospikeConnection.getSupportedType(String.class));
                }
                // Since this the first insertion, a null value with allowNullProperties is irrelevant, because there is no
                // properties to remove, so just ignore.
        }

        // Write vertex property to Aerospike.
        final FireflyVertexProperty<V> fireflyVertexProperty = new PackedVertexProperty<>(
                this, idValue, (PackedVertex) vertex, key, value, properties, typeHints);

        // Append vertex property to vertex.
        vertex.writeVertexProperty(fireflyVertexProperty);

        // Return FireflyVertexProperty.
        return fireflyVertexProperty;
    }
}
