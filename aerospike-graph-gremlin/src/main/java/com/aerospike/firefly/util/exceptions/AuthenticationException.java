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

public class AuthenticationException extends AerospikeGraphException {
    private AuthenticationException(final GraphError error) {
        super(error);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }

    public static AuthenticationException authNotInitialized() {
        return new AuthenticationException(AUTH_NOT_INITIALIZED);
    }

    public static AuthenticationException tokenExpired() {
        return new AuthenticationException(AUTH_TOKEN_EXPIRED);
    }

    public static AuthenticationException invalidUserContext() {
        return new AuthenticationException(AUTH_USER_CONTEXT_INVALID);
    }

    public static AuthenticationException credentialsProvidedAuthenticationDisabled() {
        return new AuthenticationException(AUTH_DISABLED_CREDENTIALS);
    }

    public static AuthenticationException userNotFoundInParameters() {
        return new AuthenticationException(AUTH_USER_PARAM_NOT_FOUND);
    }

    public static AuthenticationException userDoesNotHaveValidRole() {
        return new AuthenticationException(AUTH_USER_INVALID_ROLE);
    }

    public static AuthenticationException userDoesNotHaveWriteAccess() {
        return new AuthenticationException(AUTH_USER_WRITE_REQUIRED);
    }

    public static AuthenticationException userDoesNotHaveAdminAccess() {
        return new AuthenticationException(AUTH_USER_ADMIN_REQUIRED);
    }

    public static AuthenticationException userDoesNotHaveReadAccess() {
        return new AuthenticationException(AUTH_USER_READ_REQUIRED);
    }
}
