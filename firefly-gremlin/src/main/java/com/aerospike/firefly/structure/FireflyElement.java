package com.aerospike.firefly.structure;

import com.aerospike.client.Record;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.wrapped.WrappedElement;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public abstract class FireflyElement implements WrappedElement<Record>, Element {
    public final FireflyId id;
    protected String label;
    protected boolean removed = false;
    protected final boolean allowNullPropertyValues = false;

    protected FireflyElement(final FireflyId id, final String label) {
        this.id = id;
        this.label = label;
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
