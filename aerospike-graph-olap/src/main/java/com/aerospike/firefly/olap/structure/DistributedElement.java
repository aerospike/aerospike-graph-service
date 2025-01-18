package com.aerospike.firefly.olap.structure;

import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Iterator;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public abstract class DistributedElement<V> implements Element {
    static final String ID_STRING = "~id";
    static final String LABEL_STRING = "~label";
    static final String PROPERTIES_STRING = "~properties";
    static final String IN_STRING = "~in";
    static final String OUT_STRING = "~out";

    private final String id;
    private final String label;

    public DistributedElement(final Object id, final String label) {
        this.id = id.toString();
        this.label = label;
    }

    @Override
    public Object id() {
        return id;
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public Graph graph() {
        return null;
    }

    @Override
    public void remove() {
    }

    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    @Override
    public boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }
}
