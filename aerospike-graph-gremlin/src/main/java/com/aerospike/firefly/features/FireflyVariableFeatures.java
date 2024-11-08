package com.aerospike.firefly.features;

import org.apache.tinkerpop.gremlin.structure.Graph;

class FireflyVariableFeatures extends FireflyPropertyFeatures implements Graph.Features.VariableFeatures {
    /**
     * If any of the features on {@link Graph.Features.VariableFeatures} is {@code true} then this value must be {@code true}.
     */
    @Override
    public boolean supportsVariables() {
        return supportsBooleanValues() || supportsByteValues() || supportsDoubleValues() || supportsFloatValues()
                || supportsIntegerValues() || supportsLongValues() || supportsMapValues()
                || supportsMixedListValues() || supportsSerializableValues()
                || supportsStringValues() || supportsUniformListValues() || supportsBooleanArrayValues()
                || supportsByteArrayValues() || supportsDoubleArrayValues() || supportsFloatArrayValues()
                || supportsIntegerArrayValues() || supportsLongArrayValues() || supportsStringArrayValues();
    }
}
