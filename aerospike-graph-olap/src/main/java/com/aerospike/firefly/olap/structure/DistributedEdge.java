package com.aerospike.firefly.olap.structure;

import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;


/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * Most of the Distributed* classes are adapted from the Spark* in TinkerPop from by Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DistributedEdge extends DistributedElement implements Edge {

    public DistributedEdge(final Object id, final String label) {
        super(id.toString(), 0, label);
    }

    @Override
    public Iterator<Vertex> vertices(final Direction direction) {
        return null;
    }

    @Override
    public Vertex outVertex() {
        return null;
    }

    @Override
    public Vertex inVertex() {
        return null;
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        return null;
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        return null;
    }

    @Override
    public String toString() {
        return null;
    }
}
