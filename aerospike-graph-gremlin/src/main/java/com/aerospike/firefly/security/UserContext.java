package com.aerospike.firefly.security;

import com.aerospike.firefly.structure.FireflyGraph;

import java.util.List;

public interface UserContext {
    enum ROLE{
        ADMIN,WRITE,READ
    }
    List<ROLE> getRoles();
    boolean valid(FireflyGraph graph);
}
