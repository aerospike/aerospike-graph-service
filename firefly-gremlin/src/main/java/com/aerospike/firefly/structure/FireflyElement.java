package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.FireflyRecord;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public abstract class FireflyElement implements Element {
    protected final FireflyId id;
    protected final String label;
    protected final FireflyRecord record;
    protected boolean removed = false;
    protected final boolean allowNullPropertyValues = false;


    protected FireflyElement(final FireflyId id, final String label, FireflyRecord record) {
        this.id = id;
        this.label = label;
        this.record = record;
    }

    @Override
    public Object id() {
        return this.id.value();
    }

    @Override
    public String label() {
        return this.label;
    }

    protected static IllegalStateException elementAlreadyRemoved(final Class<? extends Element> clazz, final Object id) {
        return new IllegalStateException(String.format("%s with id %s was removed.", clazz.getSimpleName(), id));
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        int hashCode = ElementHelper.hashCode(this);
        return hashCode;
    }
}
