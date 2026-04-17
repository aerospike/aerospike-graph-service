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

public class IntegerConfigValidator extends NumericConfigValidator<Integer> {

    public IntegerConfigValidator() {
        super(Integer.MIN_VALUE, Integer.MAX_VALUE);
    }

    @Override
    protected Number parseValue(final String value) {
        return Integer.parseInt(value);
    }

    @Override
    protected Integer getValue(final Number number) {
        return number.intValue();
    }

    @Override
    protected boolean underMinimum(final Number parsedValue, final String key) {
        return parsedValue.intValue() < this.minimums.get(key).intValue();
    }

    @Override
    protected boolean overMaximum(final Number parsedValue, final String key) {
        return parsedValue.intValue() > this.maximums.get(key).intValue();
    }
}
