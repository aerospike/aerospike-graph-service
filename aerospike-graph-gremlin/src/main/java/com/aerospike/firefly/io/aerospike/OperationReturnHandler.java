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

package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.Record;

public class OperationReturnHandler {

    /**
     * Multiple Operations on the same bin in one Operate return their results contained in a List. This helper
     * method extracts the desired value when given the index at which it is returned.
     *
     * @param operationReturnValue The Record returned from the Operate.
     * @param binName              The name of the bin containing the desired value.
     * @param index                The index in the nested list of return values of Operations on binName to extract.
     * @return                     The value at the specified index in the list of return values.
     */
    public static Object getValueAtIndex(final Record operationReturnValue, final String binName, final int index) {
        return operationReturnValue.getList(binName).get(index);
    }

    /**
     * Returns the last operation result for a bin when multiple operations target the same bin in one Operate call.
     * A single GET on a bin returns the bin value directly; multiple operations return a list of results.
     */
    public static Object getLastOperationResult(final Record operationReturnValue, final String binName) {
        if (operationReturnValue == null || !operationReturnValue.bins.containsKey(binName)) {
            return null;
        }
        final Object value = operationReturnValue.getValue(binName);
        if (value instanceof java.util.List) {
            final java.util.List<?> results = (java.util.List<?>) value;
            return results.isEmpty() ? null : results.get(results.size() - 1);
        }
        return value;
    }
}
