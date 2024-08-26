package com.aerospike.firefly.security;

import com.aerospike.firefly.io.aerospike.admin.AuthenticationException;
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
    public Bytecode authorize(final AuthenticatedUser user, final Bytecode bytecode, final Map<String, String> aliases) {
        final JWTAuthenticator.JWTAuthenticatedUser jwtUser = (JWTAuthenticator.JWTAuthenticatedUser) user;
        if (!jwtUser.valid()) {
            throw AuthenticationException.tokenExpired();
        }
        // TODO: Arguments should now match the below authorize function.
        bytecode.addStep(GraphTraversal.Symbols.call, RESERVED_CALL_STRING);
        bytecode.addStep(GraphTraversal.Symbols.with, "user", user);
        return bytecode;
    }

    @Override
    public void authorize(final AuthenticatedUser user, final RequestMessage msg) {
        final JWTAuthenticator.JWTAuthenticatedUser jwtUser = (JWTAuthenticator.JWTAuthenticatedUser) user;
        if (!jwtUser.valid()) {
            throw AuthenticationException.tokenExpired();
        }
        final Map<String, Object> arguments = msg.getArgs();
        final String gremlinString = arguments.get("gremlin") +
                String.format(".call('aerospike.graph.admin.reserved.info').with('name', '%s').with('role', '%s')",
                        jwtUser.getName(), jwtUser.getRole().toString());
        arguments.put("gremlin", gremlinString);
    }
}
