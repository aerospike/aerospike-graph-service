package com.aerospike.firefly.structure;

import com.aerospike.client.Record;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public abstract class FireflyVertexProperty<V> extends FireflyElement implements VertexProperty<V> {

    protected final boolean allowNullPropertyValues = true;
    protected final FireflyId vertexId;
    protected final String key;
    protected final V value;
    protected final FireflyGraph graph;


    public FireflyVertexProperty(final FireflyGraph graph, final FireflyId id, final FireflyId vertexId, final String key, final V value, final Object... propertyKeyValues) {
        super(id, key);
        if (!allowNullPropertyValues && null == value)
            throw new IllegalArgumentException("value cannot be null as feature supportsNullPropertyValues is false");
        this.graph = graph;
        this.vertexId = vertexId;
        this.key = key;
        this.value = value;
        ElementHelper.legalPropertyKeyValueArray(propertyKeyValues);
        ElementHelper.attachProperties(this, propertyKeyValues);
    }

    public FireflyVertexProperty(final FireflyGraph graph, final FireflyId fid, final FireflyId vertexId, String key, V value) {
        super(fid, key);
        this.graph = graph;
        if (!allowNullPropertyValues && null == value)
            throw new IllegalArgumentException("value cannot be null as feature supportsNullPropertyValues is false");

        this.vertexId = vertexId;
        this.key = key;
        this.value = value;
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public Vertex element() {
        return graph.readVertex(vertexId);
    }

    @Override
    public <U> Property<U> property(final String key, final U value) {
        if (this.removed) throw elementAlreadyRemoved(VertexProperty.class, id);


        if ((!allowNullPropertyValues && null == value)) {
            properties(key).forEachRemaining(Property::remove);
            return Property.empty();
        }

        return graph.writeVertexProperty(this.vertexId, graph.readVertex(vertexId), key, value);
    }

    @Override
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        if (propertyKeys.length == 1) {
            final Property<V> property = graph.readVertex(vertexId).property(propertyKeys[0]);
            return null == property ? Collections.emptyIterator() : IteratorUtils.of(property);
        } else {
            Iterator<Map.Entry<String, VertexProperty<Object>>> vertexProperties = graph.readVertex(vertexId).readVertexProperties();
            return IteratorUtils.map(
                    IteratorUtils.filter(
                            IteratorUtils.asIterator(vertexProperties),
                    entry -> ElementHelper.keyExists((String) ((AbstractMap.Entry) entry).getKey(), propertyKeys)),
                    entry -> ((AbstractMap.Entry) entry).getValue());
        }
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public Record getBaseElement() {
        return FireflyRecord.read(graph.getBaseGraph(), graph.getBaseGraph().VERTEX_PROPERTY_AERO_SET, FireflyId.fromElement(this)).record();
    }
}

