package com.aerospike.firefly.security;

import com.aerospike.firefly.structure.FireflyGraph;

public interface UserContext {
    enum ROLE{
        ADMIN,
        WRITE,
        READ
    }
    ROLE getRole();
    boolean valid(FireflyGraph graph);
}
