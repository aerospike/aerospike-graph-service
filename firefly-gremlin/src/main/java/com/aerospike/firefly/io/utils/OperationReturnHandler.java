package com.aerospike.firefly.io.utils;

import com.aerospike.client.Record;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
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
}
