package com.aerospike.firefly.util.exceptions;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;

import static com.aerospike.firefly.util.exceptions.GraphError.clientResultCodeToEnum;

public class AerospikeGraphException extends RuntimeException {
    public final int errorCode;

    private AerospikeGraphException(final AerospikeException cause) {
        super(GraphError.getMessage(cause), cause);
        if (cause.getResultCode() < 0) {
            errorCode = clientResultCodeToEnum(cause.getResultCode());
        } else {
            errorCode = cause.getResultCode();
        }
    }

    public AerospikeGraphException(final GraphError error) {
        super(GraphError.getMessage(error));
        this.errorCode = error.code;
    }

    protected AerospikeGraphException(final GraphError error, final AerospikeException cause) {
        super(GraphError.getMessage(error), cause);
        this.errorCode = error.code;
    }

    static public AerospikeGraphException fromAerospikeException(final AerospikeException ae) {
        switch (ae.getResultCode()) {
            case ResultCode.RECORD_TOO_BIG:
                return new AerospikeGraphRecordSizeExceededException(ae);
            case ResultCode.KEY_NOT_FOUND_ERROR:
                return new AerospikeGraphElementNotFoundException();
            default:
                return new AerospikeGraphException(ae);
        }
    }
}
