package com.aerospike.firefly.security;

import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;

public interface UserContext {
    enum ROLE {
        ADMIN,
        READ_WRITE,
        READ
    }
    ROLE getRole() throws AuthenticationException;
    boolean valid();
}
