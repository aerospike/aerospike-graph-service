package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.io.aerospike.admin.AuthenticationException;
import com.aerospike.firefly.security.JWTAuthenticator;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CallStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.translator.GroovyTranslator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.aerospike.firefly.io.aerospike.admin.AdminServiceRegistry.RESERVED_USER_CONTEXT;
import static com.aerospike.firefly.security.JWTAuthorizer.RESERVED_CALL_STRING;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyAuthenticationStrategy extends FireflyStrategyBase {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyAuthenticationStrategy.class);
    boolean hasMutateStep = false;
    JWTAuthenticator.JWTAuthenticatedUser jwtUser = null;

    /**
     * Default constructor for FireflyAuthenticationStrategy.
     */
    public FireflyAuthenticationStrategy() {
    }

    @Override
    public String getStrategyEnabledKey() {
        return null;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();
        final List<CallStep> callSteps = new ArrayList<>();
        CallStep adminStep = null;
        if (!graph.getBaseGraph().AUTHENTICATION_ENABLED) {
            for (final Step step : traversal.getSteps()) {
                if (!(step instanceof CallStep)) {
                    continue;
                }
                final CallStep callStep = (CallStep) step;
                String serviceName = null;
                // The serviceName is private, need to use reflection to get it so the compiler doesn't complain.
                try {
                    final Field serviceNameField = CallStep.class.getDeclaredField("serviceName");
                    serviceNameField.setAccessible(true);
                    serviceName = (String) serviceNameField.get(callStep);
                } catch (final NoSuchFieldException | IllegalAccessException ignore) {
                }

                if (RESERVED_CALL_STRING.equals(serviceName)) {
                    throw AuthenticationException.credentialsProvidedAuthenticationDisabled();
                }
            }
            return;
        }
        for (final Step step : traversal.getSteps()) {
            if (!(step instanceof CallStep)) {
                if (step instanceof Mutating) {
                    hasMutateStep = true;
                }
                continue;
            }
            final CallStep callStep = (CallStep) step;
            String serviceName = null;
            // The serviceName is private, need to use reflection to get it so the compiler doesn't complain.
            try {
                final Field serviceNameField = CallStep.class.getDeclaredField("serviceName");
                serviceNameField.setAccessible(true);
                serviceName = (String) serviceNameField.get(callStep);
            } catch (final NoSuchFieldException | IllegalAccessException ignore) {
            }

            if (!RESERVED_CALL_STRING.equals(serviceName)) {
                callSteps.add(callStep);
                continue;
            }
            try {
                final Field parametersField = CallStep.class.getDeclaredField("parameters");
                parametersField.setAccessible(true);
                final Parameters parameters = (Parameters) parametersField.get(callStep);
                final Map<Object, List<Object>> params = parameters.getRaw();
                if (!params.containsKey("user")
                        || params.get("user").size() != 1 ||
                        !(params.get("user").get(0) instanceof JWTAuthenticator.JWTAuthenticatedUser)) {
                    throw AuthenticationException.userNotFoundInParameters();
                }

                jwtUser = (JWTAuthenticator.JWTAuthenticatedUser) params.get("user").get(0);
                adminStep = callStep;
            } catch (final IllegalAccessException | NoSuchFieldException e) {
                throw new RuntimeException(e);
            }
        }

        // Add token.
        callSteps.forEach(callStep -> callStep.configure(RESERVED_USER_CONTEXT, jwtUser));

        // Remove admin step.
        if (adminStep != null) {
            traversal.removeStep(adminStep);
        }

        if (jwtUser == null) {
            throw AuthenticationException.userNotFoundInParameters();
        } else {
            if (!jwtUser.valid()) {
                throw AuthenticationException.tokenExpired();
            }
            final UserContext.ROLE role = jwtUser.getRole();
            if (role == null) {
                throw AuthenticationException.userDoesNotHaveValidRole();
            }
            // Admin steps are call steps. These have internal auth checks.
            if (hasMutateStep) {
                if (!role.equals(UserContext.ROLE.READ_WRITE) && !role.equals(UserContext.ROLE.ADMIN)) {
                    if (graph.getBaseGraph().IS_AUDIT_LOG_ENABLED) {
                        // The clone doesn't take the reserved step.
                        final Traversal copy = traversal.clone();
                        final List<Bytecode.Instruction> instructions = copy.asAdmin().getBytecode().getStepInstructions();
                        final List<Bytecode.Instruction> toRemove = new ArrayList<>();
                        boolean found = false;
                        for (final Bytecode.Instruction instruction : instructions) {
                            // If we removed the previous instruction, this one needs to be removed too. Call and with are separate in the bytecode.
                            if (found) {
                                toRemove.add(instruction);
                            }
                            if (instruction.getOperator().equals("call") && instruction.getArguments().length > 0 &&
                                    instruction.getArguments()[0].equals(RESERVED_CALL_STRING)) {
                                toRemove.add(instruction);
                                found = true;
                            }
                        }
                        toRemove.forEach(instructions::remove);
                        LOG.info("{{}} - " + " Insufficient permissions to execute mutating step. Query: 'g{}'.", jwtUser.getName(),
                                GroovyTranslator.of("").translate(copy.asAdmin().getBytecode()).getScript());
                    }
                    throw AuthenticationException.userDoesNotHaveWriteAccess();
                }
            }
            if (!hasMutateStep) {
                if (!role.equals(UserContext.ROLE.READ) &&
                        !role.equals(UserContext.ROLE.READ_WRITE) &&
                        !role.equals(UserContext.ROLE.ADMIN)) {
                    throw AuthenticationException.userDoesNotHaveReadAccess();
                }
            }
        }
        graph.setUser(jwtUser.getName());
    }

    @Override
    public void reset() {
        hasMutateStep = false;
        jwtUser = null;
    }
}
