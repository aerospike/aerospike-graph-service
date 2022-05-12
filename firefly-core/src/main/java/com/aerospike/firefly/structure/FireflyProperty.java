package com.aerospike.firefly.structure;

import com.aerospike.client.Record;
import com.aerospike.firefly.util.Exceptions.Unimplemented;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyProperty<V> implements Property<V> {
    public static final String AERO_SET = FireflyVertexProperty.class.getSimpleName().toUpperCase();
    public static final String ELEMENT_PROPERTIES = "_EP";
    private final FireflyElement element;
    private final String key;
    private final V value;

    public FireflyProperty(FireflyElement element, String key, V value) {
        this.element = element;
        this.key = key;
        this.value = value;
    }

    @Override
    public String key() {
        return this.key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return this.value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public Element element() {
        return this.element;
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public void remove() {
        this.element.removeProperty(this.key);
    }
    @Override
    public String toString(){
        return StringFactory.propertyString(this);
    }
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

}
