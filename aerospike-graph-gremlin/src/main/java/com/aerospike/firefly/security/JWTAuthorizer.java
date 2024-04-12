package com.aerospike.firefly.security;

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.authz.AuthorizationException;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;

import java.util.Map;

public class JWTAuthorizer implements Authorizer {
    public static final String RESERVED_CALL_STRING = "aerospike.graph.admin.reserved.info";
    @Override
    public void setup(final Map<String, Object> config) throws AuthorizationException {
    }

    @Override
    public Bytecode authorize(final AuthenticatedUser user, final Bytecode bytecode, final Map<String, String> aliases) throws AuthorizationException {
        bytecode.addStep(GraphTraversal.Symbols.call, RESERVED_CALL_STRING);
        bytecode.addStep(GraphTraversal.Symbols.with, "user", user);
        return bytecode;
    }

    @Override
    public void authorize(final AuthenticatedUser user, final RequestMessage msg) throws AuthorizationException {
        throw new RuntimeException("cannot authorize script");
    }
}
