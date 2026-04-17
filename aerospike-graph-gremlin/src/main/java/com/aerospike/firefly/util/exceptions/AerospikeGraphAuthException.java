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

import static com.aerospike.firefly.util.exceptions.GraphError.AUTH_DISABLED_CREDENTIALS;
import static com.aerospike.firefly.util.exceptions.GraphError.AUTH_NOT_INITIALIZED;
import static com.aerospike.firefly.util.exceptions.GraphError.AUTH_TOKEN_EXPIRED;
import static com.aerospike.firefly.util.exceptions.GraphError.AUTH_USER_ADMIN_REQUIRED;
import static com.aerospike.firefly.util.exceptions.GraphError.AUTH_USER_CONTEXT_INVALID;
import static com.aerospike.firefly.util.exceptions.GraphError.AUTH_USER_INVALID_ROLE;
import static com.aerospike.firefly.util.exceptions.GraphError.AUTH_USER_PARAM_NOT_FOUND;
import static com.aerospike.firefly.util.exceptions.GraphError.AUTH_USER_READ_REQUIRED;
import static com.aerospike.firefly.util.exceptions.GraphError.AUTH_USER_WRITE_REQUIRED;

public class AerospikeGraphAuthException extends AerospikeGraphException {
    private AerospikeGraphAuthException(final GraphError error) {
        super(error);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }

    public static AerospikeGraphAuthException authNotInitialized() {
        return new AerospikeGraphAuthException(AUTH_NOT_INITIALIZED);
    }

    public static AerospikeGraphAuthException tokenExpired() {
        return new AerospikeGraphAuthException(AUTH_TOKEN_EXPIRED);
    }

    public static AerospikeGraphAuthException invalidUserContext() {
        return new AerospikeGraphAuthException(AUTH_USER_CONTEXT_INVALID);
    }

    public static AerospikeGraphAuthException credentialsProvidedAuthenticationDisabled() {
        return new AerospikeGraphAuthException(AUTH_DISABLED_CREDENTIALS);
    }

    public static AerospikeGraphAuthException userNotFoundInParameters() {
        return new AerospikeGraphAuthException(AUTH_USER_PARAM_NOT_FOUND);
    }

    public static AerospikeGraphAuthException userDoesNotHaveValidRole() {
        return new AerospikeGraphAuthException(AUTH_USER_INVALID_ROLE);
    }

    public static AerospikeGraphAuthException userDoesNotHaveWriteAccess() {
        return new AerospikeGraphAuthException(AUTH_USER_WRITE_REQUIRED);
    }

    public static AerospikeGraphAuthException userDoesNotHaveAdminAccess() {
        return new AerospikeGraphAuthException(AUTH_USER_ADMIN_REQUIRED);
    }

    public static AerospikeGraphAuthException userDoesNotHaveReadAccess() {
        return new AerospikeGraphAuthException(AUTH_USER_READ_REQUIRED);
    }
}
