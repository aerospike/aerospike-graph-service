package com.aerospike.firefly.process.computer;

import com.aerospike.firefly.structure.FireflyVertex;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;

import static com.aerospike.firefly.util.Tokens.UNIMPLEMENTED;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyGraphComputerView {
    public <V> Property<V> addProperty(final FireflyVertex vertex, final String key, final V value) {
        throw new UnsupportedOperationException(UNIMPLEMENTED);
    }

    public boolean legalEdge(FireflyVertex fireflyVertex, Edge edge) {
        throw new UnsupportedOperationException(UNIMPLEMENTED);
    }
}
