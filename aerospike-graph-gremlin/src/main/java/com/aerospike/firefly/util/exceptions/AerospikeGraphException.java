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

    protected AerospikeGraphException(final GraphError error, final String message) {
        super(message);
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
