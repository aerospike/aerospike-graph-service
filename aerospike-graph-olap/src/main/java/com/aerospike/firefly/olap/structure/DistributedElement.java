package com.aerospike.firefly.olap.structure;

import com.aerospike.firefly.olap.codec.Codec;
import com.aerospike.firefly.olap.codec.RowCodec;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * Most of the Distributed* classes are adapted from the Spark* in TinkerPop from by Marko A. Rodriguez (http://markorodriguez.com)
 */
public abstract class DistributedElement<V> implements Element {
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
        if (RowCodec.ID_TYPE.STRING.ordinal() == idTypeOrdinal) {
            return id;
        } else if (RowCodec.ID_TYPE.LONG.ordinal() == idTypeOrdinal) {
            return Long.parseLong(id);
        } else if (RowCodec.ID_TYPE.INTEGER.ordinal() == idTypeOrdinal) {
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
