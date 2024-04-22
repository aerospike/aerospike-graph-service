package com.aerospike.firefly.security;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticationException;

public interface UserContext {
    enum ROLE {
        ADMIN,
        READ_WRITE,
        READ
    }
    ROLE getRole() throws AuthenticationException;
    boolean valid(FireflyGraph graph);
}
