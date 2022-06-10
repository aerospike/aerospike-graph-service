package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.FireflyRecord;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.structure.Graph.Hidden.isHidden;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyEdge extends FireflyElement implements Edge {
    protected boolean removed;
    private final FireflyGraph graph;
    private final FireflyId inVid;
    private final FireflyId outVid;

    private void writeProperty(String k, Object v) {
        this.graph.db.writeProperty(this.id, this.getClass(), k, v);
    }

    private Map<String, Property> readProperties() {
        return this.graph.db.readProperties(this);
    }


    public FireflyEdge(FireflyRecord record, FireflyId id, String label, FireflyId outVid, FireflyId inVid, FireflyGraph graph) {
        super(id, label, record);
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
        FireflyHelper.legalPropertyKeyValueArray(key, value);
        if (isHidden(key))
            throw Edge.Exceptions.labelCanNotBeAHiddenKey(key);
        if (this.removed) throw elementAlreadyRemoved(Edge.class, id);
        if ((!allowNullPropertyValues && null == value)) {
            properties(key).forEachRemaining(Property::remove);
            return Property.empty();
        }
        this.writeProperty(key, value);
        return new FireflyProperty<>(this, key, value);
    }

    @Override
    public void remove() {
        graph.db.removeEdgeFromVertex(graph, (FireflyVertex) this.outVertex(), this, Direction.IN);
        graph.db.removeEdgeFromVertex(graph, (FireflyVertex) this.outVertex(), this, Direction.OUT);
        graph.db.removeEdgeFromVertex(graph, (FireflyVertex) this.inVertex(), this, Direction.IN);
        graph.db.removeEdgeFromVertex(graph, (FireflyVertex) this.inVertex(), this, Direction.OUT);
        graph.db.removeEdge(graph, this.id);
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        Map<String, Property> properties = this.readProperties();
        if (propertyKeys.length == 1) {
            final Property<V> property = properties.get(propertyKeys[0]);
            return null == property ? Collections.emptyIterator() : IteratorUtils.of(property);
        } else {
            return IteratorUtils.map(IteratorUtils.filter(IteratorUtils.asIterator(properties.entrySet()),
                    entry -> ElementHelper.keyExists((String) ((AbstractMap.Entry) entry).getKey(), propertyKeys)), entry -> ((AbstractMap.Entry) entry).getValue());
        }
    }


    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

}
