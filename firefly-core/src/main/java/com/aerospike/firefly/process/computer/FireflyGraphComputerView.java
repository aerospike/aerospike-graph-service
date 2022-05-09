package com.aerospike.firefly.process.computer;

import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.Exceptions;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyGraphComputerView {
    public <V> Property<V> addProperty(final FireflyVertex vertex, final String key, final V value) {
        throw new Exceptions.Unimplemented();
    }

    public boolean legalEdge(FireflyVertex fireflyVertex, Edge edge) {
        throw new Exceptions.Unimplemented();
    }
}
