package com.aerospike.firefly.io.utils;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;

public class GenerationCheck {
    private static final int GENERATION_WRITE_FAILURES = 100;
    public static void writeGenerationCheck(GenerationCheckFunction f) {
        int i = 0;
        do {
            i++;
            try {
                f.function();
            } catch (AerospikeException e) {
                if (e.getResultCode() == ResultCode.GENERATION_ERROR) {
                    continue;
                } else {
                    throw e;
                }
            }
            break;
        } while (i <= GENERATION_WRITE_FAILURES);
    }

    public interface GenerationCheckFunction {
        void function();
    }
}
