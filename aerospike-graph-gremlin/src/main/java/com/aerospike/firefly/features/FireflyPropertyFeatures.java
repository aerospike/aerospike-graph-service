package com.aerospike.firefly.features;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;

abstract class FireflyPropertyFeatures implements Graph.Features.PropertyFeatures {
    /**
     * Supports setting of a boolean value.
     */
    @Override
    public boolean supportsBooleanValues() {
        return true;
    }

    /**
     * Supports setting of a byte value.
     */
    @Override
    public boolean supportsByteValues() {
        return false;
    }

    /**
     * Supports setting of a double value.
     */
    @Override
    public boolean supportsDoubleValues() {
        return true;
    }

    /**
     * Supports setting of a float value.
     */
    @Override
    public boolean supportsFloatValues() {
        return false;
    }

    /**
     * Supports setting of a integer value.
     */
    @Override
    public boolean supportsIntegerValues() {
        return true;
    }

    /**
     * Supports setting of a long value.
     */
    @Override
    public boolean supportsLongValues() {
        return true;
    }

    /**
     * Supports setting of a {@code Map} value.  The assumption is that the {@code Map} can contain
     * arbitrary serializable values that may or may not be defined as a feature itself.
     */
    @Override
    public boolean supportsMapValues() {
        return false;
    }

    /**
     * Supports setting of a {@code List} value.  The assumption is that the {@code List} can contain
     * arbitrary serializable values that may or may not be defined as a feature itself.  As this
     * {@code List} is "mixed" it does not need to contain objects of the same type.
     *
     * @see #supportsMixedListValues()
     */
    @Override
    public boolean supportsMixedListValues() {
        return false;
    }

    /**
     * Supports setting of an array of boolean values.
     */
    @Override
    public boolean supportsBooleanArrayValues() {
        return true;
    }

    /**
     * Supports setting of an array of byte values.
     */
    @Override
    public boolean supportsByteArrayValues() {
        return true;
    }

    /**
     * Supports setting of an array of double values.
     */
    @Override
    public boolean supportsDoubleArrayValues() {
        return true;
    }

    /**
     * Supports setting of an array of float values.
     */
    @Override
    public boolean supportsFloatArrayValues() {
        return false;
    }

    /**
     * Supports setting of an array of integer values.
     */
    @Override
    public boolean supportsIntegerArrayValues() {
        return true;
    }

    /**
     * Supports setting of an array of string values.
     */
    @Override
    public boolean supportsStringArrayValues() {
        return true;
    }

    /**
     * Supports setting of an array of long values.
     */
    @Override
    public boolean supportsLongArrayValues() {
        return true;
    }

    /**
     * Supports setting of a Java serializable value.
     */
    @Override
    public boolean supportsSerializableValues() {
        return false;
    }

    /**
     * Supports setting of a string value.
     */
    @Override
    public boolean supportsStringValues() {
        return true;
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
        return true;
    }

    /**
     * Determines if an {@link Element} allows for the processing of at least one data type defined by the
     * features.  In this case "processing" refers to at least "reading" the data type. If any of the
     * features on {@link Graph.Features.PropertyFeatures} is true then this value must be true.
     */
    @Override
    public boolean supportsProperties() {
        return supportsBooleanValues() || supportsByteValues() || supportsDoubleValues() || supportsFloatValues()
                || supportsIntegerValues() || supportsLongValues() || supportsMapValues()
                || supportsMixedListValues() || supportsSerializableValues()
                || supportsStringValues() || supportsUniformListValues() || supportsBooleanArrayValues()
                || supportsByteArrayValues() || supportsDoubleArrayValues() || supportsFloatArrayValues()
                || supportsIntegerArrayValues() || supportsLongArrayValues() || supportsStringArrayValues();
    }
}
