package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.io.aerospike.admin.AuthenticationException;
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
    final ThreadLocal<UsernameRolePair> usernameRolePair = ThreadLocal.withInitial(() -> null);
    final ThreadLocal<Boolean> hasMutateStep = ThreadLocal.withInitial(() -> false);

    public static class UsernameRolePair {
        private final String username;
        private final UserContext.ROLE role;

        private UsernameRolePair(String username, UserContext.ROLE role) {
            this.username = username;
            this.role = role;
        }

        public String getUsername() {
            return username;
        }

        public UserContext.ROLE getRole() {
            return role;
        }
    }

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
                    hasMutateStep.set(true);
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

            final Parameters parameters = callStep.getParameters();
            final Map<Object, List<Object>> params = parameters.getRaw();
            if (!params.containsKey("name") || !params.containsKey("role") ||
                    params.get("name").size() != 1 || params.get("role").size() != 1) {
                throw AuthenticationException.userNotFoundInParameters();
            }
            final String username = (String) params.get("name").get(0);
            final UserContext.ROLE role = UserContext.ROLE.valueOf((String) params.get("role").get(0));
            usernameRolePair.set(new UsernameRolePair(username, role));
            adminStep = callStep;
        }

        // Add token.
        callSteps.forEach(callStep -> callStep.configure(RESERVED_USER_CONTEXT, usernameRolePair.get()));

        // Remove admin step.
        if (adminStep != null) {
            traversal.removeStep(adminStep);
        }

        if (usernameRolePair.get() == null) {
            throw AuthenticationException.userNotFoundInParameters();
        } else {
            final UserContext.ROLE role = usernameRolePair.get().getRole();
            if (role == null) {
                throw AuthenticationException.userDoesNotHaveValidRole();
            }
            // Admin steps are call steps. These have internal auth checks.
            if (hasMutateStep.get()) {
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
                        LOG.info("[{}] - " + " Insufficient permissions to execute mutating step. Query: 'g{}'.", usernameRolePair.get().getUsername(),
                                GroovyTranslator.of("").translate(copy.asAdmin().getBytecode()).getScript());
                    }
                    throw AuthenticationException.userDoesNotHaveWriteAccess();
                }
            }
            if (!hasMutateStep.get()) {
                if (!role.equals(UserContext.ROLE.READ) &&
                        !role.equals(UserContext.ROLE.READ_WRITE) &&
                        !role.equals(UserContext.ROLE.ADMIN)) {
                    throw AuthenticationException.userDoesNotHaveReadAccess();
                }
            }
        }
        graph.setUser(usernameRolePair.get().username);
    }

    @Override
    public void reset() {
        usernameRolePair.remove();
        hasMutateStep.remove();
    }
}
