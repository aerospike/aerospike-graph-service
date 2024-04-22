package com.aerospike.firefly.io.aerospike.admin;

public class AuthenticationException extends RuntimeException {
    private AuthenticationException(final String message) {
        super(message);
    }

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }

    public static AuthenticationException authNotInitialized() {
        return new AuthenticationException("Authentication is not initialized.");
    }

    public static AuthenticationException invalidUserContext() {
        return new AuthenticationException("User context is invalid.");
    }

    public static AuthenticationException credentialsProvidedAuthenticationDisabled() {
        return new AuthenticationException("Error, authentication is disabled but credentials were provided.");
    }

    public static AuthenticationException userNotFoundInParameters() {
        return new AuthenticationException("User not valid, no user found in parameters.");
    }

    public static AuthenticationException userDoesNotHaveValidRole() {
        return new AuthenticationException("User does not have a valid role.");
    }

    public static AuthenticationException userDoesNotHaveWriteAccess() {
        return new AuthenticationException("User does not have write access.");
    }

    public static AuthenticationException userDoesNotHaveAdminAccess() {
        return new AuthenticationException("User does not have admin access.");
    }

    public static AuthenticationException userDoesNotHaveReadAccess() {
        return new AuthenticationException("User does not have read access.");
    }
}
