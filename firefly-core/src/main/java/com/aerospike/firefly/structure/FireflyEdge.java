package com.aerospike.firefly.structure;

import com.aerospike.client.Record;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.Iterator;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyEdge extends FireflyElement implements Edge {
    public static final String AERO_SET = FireflyEdge.class.getSimpleName().toUpperCase();
    public boolean removed;


    public FireflyEdge(Record record, Object id, String label, long inVid, long outVid, FireflyGraph graph) {
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

    @Override
    public void removeProperty(String key) {
        ((FireflyGraph)this.graph()).db.removePropertyFromEdge(this,key);
    }
    @Override
    public String toString(){
        return StringFactory.edgeString(this);
    }
}
