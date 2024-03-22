package com.aerospike.firefly.security;

import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CallStep;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.server.authz.AuthorizationException;
import org.apache.tinkerpop.gremlin.server.authz.Authorizer;
import org.apache.tinkerpop.gremlin.util.message.RequestMessage;

import java.util.List;
import java.util.Map;

public class JWTAuthorizer implements Authorizer {
    @Override
    public void setup(final Map<String, Object> config) throws AuthorizationException {

    }

    @Override
    public Bytecode authorize(final AuthenticatedUser user, final Bytecode bytecode, final Map<String, String> aliases) throws AuthorizationException {
        final JWTAuthenticator.JWTAuthenticatedUser jwtUser = ((JWTAuthenticator.JWTAuthenticatedUser) user);
        boolean hasAdminInstructions = hasAdminInstructions(bytecode.getStepInstructions());
        boolean hasWriteInstructions = hasWriteInstructions(bytecode.getStepInstructions());
        boolean hasReadInstructions = hasReadInstructions(bytecode.getStepInstructions());

        bytecode.addStep(CallStep.class.getSimpleName(),
                JWTAuthenticator.class.getSimpleName(),
                Map.of("token",jwtUser.toString()));
        return null;
    }

    private boolean hasReadInstructions(List<Bytecode.Instruction> stepInstructions) {
        return false;
    }

    private boolean hasWriteInstructions(List<Bytecode.Instruction> stepInstructions) {
        return false;
    }

    private boolean hasAdminInstructions(List<Bytecode.Instruction> stepInstructions) {
        return false;
    }

    @Override
    public void authorize(final AuthenticatedUser user, final RequestMessage msg) throws AuthorizationException {
        throw new RuntimeException("cannot authorize script");
    }
}
