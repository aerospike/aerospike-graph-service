/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
