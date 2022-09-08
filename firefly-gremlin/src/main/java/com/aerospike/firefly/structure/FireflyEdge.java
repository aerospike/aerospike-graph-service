package com.aerospike.firefly.structure;

import com.aerospike.client.Record;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.util.FireflyHelper;
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
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public abstract class FireflyEdge extends FireflyElement implements Edge {
    public boolean removed;
    protected final FireflyGraph graph;
    protected final FireflyId inVid;
    protected final FireflyId outVid;

    public abstract void removeEdge();

    public FireflyEdge(final FireflyId id, final String label, final FireflyId outVid, final FireflyId inVid, final FireflyGraph graph) {
        super(id, label);
        this.graph = graph;
        this.inVid = inVid;
        this.outVid = outVid;
    }

    @Override
    public Vertex outVertex() {
        return graph.readVertex(this.outVid);
    }

    @Override
    public Vertex inVertex() {
        return graph.readVertex(this.inVid);
    }

    public FireflyId outVertexId() {
        return this.outVid;
    }

    public FireflyId inVertexId() {
        return this.inVid;
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
        final Map<String, Property<V>> properties = graph.readProperties(this);
        return properties == null ?
                Property.empty() :
                properties.getOrDefault(key, Property.empty());
    }

    @Override
    public <V> Property<V> property(final String key, final V value) {
        FireflyHelper.legalPropertyKeyValueArray(key, value);
        if (isHidden(key))
            throw Edge.Exceptions.labelCanNotBeAHiddenKey(key);
        if (this.removed) throw elementAlreadyRemoved(Edge.class, id);
        if ((!allowNullPropertyValues && null == value)) {
            properties(key).forEachRemaining(Property::remove);
            return Property.empty();
        }
        return graph.writeProperty(this, key, value);
    }

    @Override
    public void remove() {
        //@todo multi record transactions
        removeEdge();

        final FireflyVertex inVertex = graph.readVertex(inVid);
        final FireflyVertex outVertex = graph.readVertex(outVid);
        if (inVertex != null) {
            inVertex.removeEdge(Direction.IN, id, label);
        }
        if (outVertex != null) {
            outVertex.removeEdge(Direction.OUT, id, label);
        }
        removed = true;
    }

    @Override
    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        Map<String, Property<V>> properties = graph.readProperties(this);
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

    @Override
    public Record getBaseElement() {
        return FireflyRecord.read(graph.getBaseGraph(),graph.getBaseGraph().EDGE_AERO_SET,FireflyId.fromElement(this)).record();
    }
}
