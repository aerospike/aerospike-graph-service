package com.aerospike.firefly.io.utils;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.firefly.io.AerospikeConnection;
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
    public static final String RECORD_TOO_BIG_ERROR = "Error: Vertex / edge exceeded max size. " +
            "This can be due to too many properties / edges added to an element. Consider breaking " +
            "this vertex / edge into more elements.";

    public static void writeGenerationCheck(final GenerationCheckFunction f) {
        int i = 0;
        do {
            i++;
            try {
                f.function();
            } catch (AerospikeException e) {
                if (e.getResultCode() == ResultCode.GENERATION_ERROR) {
                    AerospikeConnection.incrementGenerationCheckRetryMetric();
                    continue;
                } else if (e.getResultCode() == ResultCode.RECORD_TOO_BIG) {
                    throw new RuntimeException(RECORD_TOO_BIG_ERROR);
                } else {
                    throw e;
                }
            }
            break;
        } while (i <= GENERATION_WRITE_FAILURES);
        if (i > GENERATION_WRITE_FAILURES) {
            LOG.error("Generation check failed after {} attempts", i);
            throw new RuntimeException("Generation check failed to write after " + GENERATION_WRITE_FAILURES + " retries");
        } else if (i > 1) {
            AerospikeConnection.setGenerationCheckHighWaterMark(i);
            LOG.debug("Generation check retried {} times before succeeding", i - 1);
        }
    }

    public interface GenerationCheckFunction {
        void function();
    }
}
