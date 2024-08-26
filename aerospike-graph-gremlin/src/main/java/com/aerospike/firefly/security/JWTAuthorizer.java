package com.aerospike.firefly.security;

import com.aerospike.firefly.io.aerospike.admin.AuthenticationException;

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.authz.AuthorizationException;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JWTAuthorizer implements Authorizer {
    public static final String RESERVED_CALL_STRING = "aerospike.graph.admin.reserved.info";
    private static final Set<String> TERMINAL_STEPS = new HashSet<>(Arrays.asList(
            "explain", "iterate", "hasNext", "tryNext", "next", "toList", "toSet", "toBulkSet"));

    @Override
    public void setup(final Map<String, Object> config) throws AuthorizationException {
    }

    @Override
    public Bytecode authorize(final AuthenticatedUser user, final Bytecode bytecode, final Map<String, String> aliases) {
        final JWTAuthenticator.JWTAuthenticatedUser jwtUser = (JWTAuthenticator.JWTAuthenticatedUser) user;
        if (!jwtUser.valid()) {
            throw AuthenticationException.tokenExpired();
        }

        bytecode.addStep(GraphTraversal.Symbols.call, RESERVED_CALL_STRING);
        bytecode.addStep(GraphTraversal.Symbols.with, "name", jwtUser.getName());
        bytecode.addStep(GraphTraversal.Symbols.with, "role", jwtUser.getRole().toString());
        return bytecode;
    }

    @Override
    public void authorize(final AuthenticatedUser user, final RequestMessage msg) {
        final JWTAuthenticator.JWTAuthenticatedUser jwtUser = (JWTAuthenticator.JWTAuthenticatedUser) user;
        if (!jwtUser.valid()) {
            throw AuthenticationException.tokenExpired();
        }
        final Map<String, Object> arguments = msg.getArgs();
        final String gremlin = ((String) arguments.get("gremlin"));
        final String injection = String.format(".call('aerospike.graph.admin.reserved.info').with('name', '%s').with('role', '%s')",
                jwtUser.getName(), jwtUser.getRole().toString());

        String updatedGremlin = gremlin;

        // need to check if optional terminal step is present
        final int pos = gremlin.lastIndexOf(".");
        if (pos > 0 && pos + 2 < gremlin.length()) {
            if (TERMINAL_STEPS.contains(gremlin.substring(pos + 1, gremlin.length() - 2))) {
                // with terminal step magic is a bit different
                updatedGremlin = gremlin.substring(0, pos) + injection + gremlin.substring(pos);
            } else {
                // without terminal step we can put our fake call step at the end of Gremlin
                updatedGremlin = gremlin + injection;
            }
        }

        arguments.put("gremlin", updatedGremlin);
    }
}
