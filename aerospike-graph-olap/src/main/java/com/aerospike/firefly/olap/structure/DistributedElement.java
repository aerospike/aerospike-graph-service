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
    static final String ID_TYPEHINT_STRING = "~id_typehint";
    static final String LABEL_STRING = "~label";
    static final String PROPERTIES_STRING = "~properties";
    static final String IN_STRING = "~in";
    static final String OUT_STRING = "~out";
    static final String HALTED_STRING = "~halted";

    enum ID_TYPE {
        STRING,
        INTEGER,
        LONG
    }

    protected final String id;
    protected final int idTypeOrdinal;
    protected final String label;

    public DistributedElement(final String id,
                              final int idTypeOrdinal,
                              final String label) {
        this.id = id;
        this.idTypeOrdinal = idTypeOrdinal;
        this.label = label;
    }

    @Override
    public Object id() {
        if (ID_TYPE.STRING.ordinal() == idTypeOrdinal) {
            return id;
        } else if (ID_TYPE.LONG.ordinal() == idTypeOrdinal) {
            return Long.parseLong(id);
        } else if (ID_TYPE.INTEGER.ordinal() == idTypeOrdinal) {
            return Integer.parseInt(id);
        } else {
            // TODO.
            throw new IllegalArgumentException("Only string int and long ids are supported in olap");
        }
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
