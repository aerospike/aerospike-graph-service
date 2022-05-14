package com.aerospike.firefly.structure;

import com.aerospike.client.Record;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyEdge extends FireflyElement implements Edge {
    protected boolean removed;
    private final FireflyGraph graph;
    private final Object inVid;
    private final Object outVid;


    private void writeProperty(String k, Property v) {
        this.graph.db.writeProperty(this, k, v);
    }

    private Map<String, Property> readProperties() {
        return this.graph.db.readProperties(this);
    }

    public FireflyEdge(Record record, Object id, String label, long inVid, long outVid, FireflyGraph graph) {
        super(id, label);
        this.graph = graph;
        this.inVid = inVid;
        this.outVid = outVid;
    }

    @Override
    public Vertex outVertex() {
        return graph.db.readVertex(graph, this.outVid);
    }

    @Override
    public Vertex inVertex() {
        return graph.db.readVertex(graph, this.inVid);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        if (removed) return Collections.emptyIterator();
        switch (direction) {
            case OUT:
                return IteratorUtils.of(this.outVertex());
            case IN:
                return IteratorUtils.of(this.inVertex());
            default:
                return IteratorUtils.of(this.outVertex(), this.inVertex());
        }
    }

    @Override
    public Graph graph() {
        return this.graph;
    }

    @Override
    public <V> Property<V> property(final String key) {
        return null == this.readProperties() ? Property.<V>empty() : this.readProperties().getOrDefault(key, Property.<V>empty());
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        if (this.removed) throw elementAlreadyRemoved(VertexProperty.class, id);
        if ((!allowNullPropertyValues && null == value)) {
            properties(key).forEachRemaining(Property::remove);
            return Property.empty();
        }
        final Property<V> property = new FireflyProperty<>(this, key, value);
        this.writeProperty(key, property);
        return property;
    }

    @Override
    public void remove() {
        graph.db.removeEdgeFromVertex(graph,this.outVid,this.id,Direction.BOTH);
        graph.db.removeEdgeFromVertex(graph,this.inVid,this.id,Direction.BOTH);
        graph.db.removeEdge(graph, this.id);
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        Map<String, Property> properties = this.readProperties();
        if (propertyKeys.length == 1) {
            final Property<V> property = properties.get(propertyKeys[0]);
            return null == property ? Collections.emptyIterator() : IteratorUtils.of(property);
        } else
            return (Iterator) properties.entrySet().stream().filter(entry -> ElementHelper.keyExists(entry.getKey(), propertyKeys)).map(entry -> entry.getValue()).collect(Collectors.toList()).iterator();
    }

    @Override
    public void removeProperty(String key) {
        ((FireflyGraph) this.graph()).db.removePropertyFromEdge(this, key);
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }
}
