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

package com.aerospike.firefly.util.config;

public class DoubleConfigValidator extends NumericConfigValidator<Double> {

    public DoubleConfigValidator() {
        super(Double.MIN_VALUE, Double.MAX_VALUE);
    }

    @Override
    protected Number parseValue(final String value) {
        return Double.parseDouble(value);
    }

    @Override
    protected Double getValue(final Number number) {
        return number.doubleValue();
    }

    @Override
    protected boolean underMinimum(final Number parsedValue, final String key) {
        return parsedValue.doubleValue() < this.minimums.get(key).doubleValue();
    }

    @Override
    protected boolean overMaximum(final Number parsedValue, final String key) {
        return parsedValue.doubleValue() > this.maximums.get(key).doubleValue();
    }
}
