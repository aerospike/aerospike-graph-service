package com.aerospike.firefly.olap.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * Most of the Distributed* classes are adapted from the Spark* in TinkerPop from by Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class DistributedElement<V> implements Element {
    static final String ID_COL = "~id";
    static final String ID_TYPEHINT_COL = "~id_typehint";
    static final String LABEL_COL = "~label";
    static final String PROPERTIES_COL = "~properties";
    static final String IN_COL = "~in";
    static final String OUT_COL = "~out";
    static final String HALTED_COL = "~halted";
    static final String REF_COL = "~ref";
    static final String STEP_COL = "~step";

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
