package com.aerospike.firefly.features;

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.UUID;

class FireflyVertexPropertyFeatures extends FireflyPropertyFeatures implements Graph.Features.VertexPropertyFeatures {

    /**
     * Supports setting of an array of boolean values.
     */
    @Override
    public boolean supportsBooleanArrayValues() {
        return false;
    }

    /**
     * Supports setting of an array of double values.
     */
    @Override
    public boolean supportsDoubleArrayValues() {
        return false;
    }

    /**
     * Supports setting of an array of integer values.
     */
    @Override
    public boolean supportsIntegerArrayValues() {
        return false;
    }

    /**
     * Supports setting of an array of string values.
     */
    @Override
    public boolean supportsStringArrayValues() {
        return false;
    }

    /**
     * Supports setting of an array of long values.
     */
    @Override
    public boolean supportsLongArrayValues() {
        return false;
    }

    /**
     * Supports setting of a {@code List} value.  The assumption is that the {@code List} can contain
     * arbitrary serializable values that may or may not be defined as a feature itself.  As this
     * {@code List} is "uniform" it must contain objects of the same type.
     *
     * @see #supportsMixedListValues()
     */

    @Override
    public boolean supportsUniformListValues() {
        return false;
    }

    /**
     * Determines if meta-properties allow for {@code null} property values.
     */
    @Override
    public boolean supportsNullPropertyValues() {
        return false;
    }

    /**
     * Determines if a {@link VertexProperty} allows properties to be removed.
     */
    @Override
    public boolean supportsRemoveProperty() {
        return true;
    }

    /**
     * Determines if a {@link VertexProperty} allows an identifier to be assigned to it.
     */
    @Override
    public boolean supportsUserSuppliedIds() {
        return false;
    }

    /**
     * Determines if an {@link VertexProperty} has numeric identifiers as their internal representation.
     */
    @Override
    public boolean supportsNumericIds() {
        return true;
    }

    /**
     * Determines if an {@link VertexProperty} has string identifiers as their internal representation.
     */
    @Override
    public boolean supportsStringIds() {
        return false;
    }

    /**
     * Determines if an {@link VertexProperty} has UUID identifiers as their internal representation.
     */
    @Override
    public boolean supportsUuidIds() {
        return false;
    }

    /**
     * Determines if an {@link VertexProperty} has a specific custom object as their internal representation.
     */
    @Override
    public boolean supportsCustomIds() {
        return false;
    }

    /**
     * Determines if an {@link VertexProperty} any Java object is a suitable identifier.  Note that this
     * setting can only return true if {@link #supportsUserSuppliedIds()} is true.
     */
    @Override
    public boolean supportsAnyIds() {
        return false;
    }

    /**
     * Determines if an identifier will be accepted by the {@link Graph}.  This check is different than
     * what identifier internally supports as defined in methods like {@link #supportsNumericIds()}.  Those
     * refer to internal representation of the identifier.  A {@link Graph} may accept an identifier that
     * is not of those types and internally transform it to a native representation.
     * <p/>
     * Note that this method only applies if {@link #supportsUserSuppliedIds()} is {@code true}. Those that
     * return {@code false} for that method can immediately return false for this one as it allows no ids
     * of any type (it generates them all).
     * <p/>
     * The default implementation will immediately return {@code false} if {@link #supportsUserSuppliedIds()}
     * is {@code false}.  If custom identifiers are supported then it will throw an exception.  Those that
     * return {@code true} for {@link #supportsCustomIds()} should override this method. If
     * {@link #supportsAnyIds()} is {@code true} then the identifier will immediately be allowed.  Finally,
     * if any of the other types are supported, they will be typed checked against the class of the supplied
     * identifier.
     */
    @Override
    public boolean willAllowId(final Object id) {
        if (!supportsUserSuppliedIds()) return false;
        if (supportsCustomIds())
            throw new UnsupportedOperationException("The default implementation is not capable of validating custom ids - please override");

        return supportsAnyIds() || (supportsStringIds() && id instanceof String)
                || (supportsNumericIds() && id instanceof Number) || (supportsUuidIds() && id instanceof UUID);
    }
}
