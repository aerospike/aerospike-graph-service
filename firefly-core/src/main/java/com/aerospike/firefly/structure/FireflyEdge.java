package com.aerospike.firefly.structure;

import com.aerospike.client.Record;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.Iterator;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyEdge extends FireflyElement implements Edge {
    public static final String AERO_SET = FireflyEdge.class.getSimpleName().toUpperCase();
    public boolean removed;


    protected FireflyEdge(Object id, String label) {
        super(id, label);
    }


    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        return null;
    }

    @Override
    public Graph graph() {
        return null;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        return null;
    }

    @Override
    public void remove() {

    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        return null;
    }
}
