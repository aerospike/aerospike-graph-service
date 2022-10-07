package com.aerospike.firefly.io.utils;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class GenerationCheck {
    private static final Logger LOG = LoggerFactory.getLogger(GenerationCheck.class);

    // We don't really want to limit these retries since generation related failures should just be retried.
    // However, infinite loops r scry :-O
    private static final int GENERATION_WRITE_FAILURES = 100000;

    public static void writeGenerationCheck(final GenerationCheckFunction f) {
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
        if (i > GENERATION_WRITE_FAILURES) {
            LOG.error("Generation check failed after {} attempts", i);
            throw new RuntimeException("Generation check failed to write after " + GENERATION_WRITE_FAILURES + " retries");
        }
    }

    public interface GenerationCheckFunction {
        void function();
    }
}
