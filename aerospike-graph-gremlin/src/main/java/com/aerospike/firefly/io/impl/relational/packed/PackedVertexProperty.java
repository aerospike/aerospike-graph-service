package com.aerospike.firefly.io.impl.relational.packed;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;

import java.util.Map;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
final public class PackedVertexProperty<V> extends FireflyVertexProperty<V> {

    /**
     * Constructor for PackedVertexProperty.
     *
     * @param graph  Graph that vertex property exists on.
     * @param id     Id of vertex property.
     * @param vertex Vertex.
     * @param key    Key of vertex property.
     * @param value  Value of vertex property.
     */
    public PackedVertexProperty(final FireflyGraph graph,
                                final FireflyId id,
                                final FireflyVertex vertex,
                                final String key,
                                final Object value,
                                final Map<String, Object> properties,
                                final Map<String, Object> typeHints) {
        super(graph, id, vertex.id, key, (V) value, properties, typeHints, vertex);
    }

}
