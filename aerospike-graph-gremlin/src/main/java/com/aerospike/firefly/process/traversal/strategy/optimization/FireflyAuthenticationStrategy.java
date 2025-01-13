package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.step.util.TraversalUtil;
import com.aerospike.firefly.util.exceptions.AerospikeGraphAuthException;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Bytecode;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CallStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.aerospike.firefly.io.aerospike.admin.AdminService.RESERVED_USER_CONTEXT;
import static com.aerospike.firefly.security.JWTAuthorizer.RESERVED_CALL_STRING;
import static com.aerospike.firefly.security.UserContext.*;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyAuthenticationStrategy extends FireflyStrategyBase {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyAuthenticationStrategy.class);
    final ThreadLocal<UserClaims> userClaims = ThreadLocal.withInitial(() -> null);
    final ThreadLocal<Boolean> hasMutateStep = ThreadLocal.withInitial(() -> false);

    public static class UserClaims {
        private final String username;
        private final ROLE role;
        private final Map<String, String> allRoles;

        private UserClaims(final String username, final ROLE graphRole, final Map<String, String> allRoles) {
            this.username = username;
            this.role = graphRole;
            this.allRoles = allRoles;
        }

        public String getUsername() {
            return username;
        }

        public ROLE getRole() {
            return role;
        }

        public ROLE getRole(final String graphId) {
            if (allRoles == null) {
                return role;
            }
            if (allRoles.containsKey(graphId)) {
                return ROLE.valueOf(allRoles.get(graphId));
            }
            return null;
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
                    throw AerospikeGraphAuthException.credentialsProvidedAuthenticationDisabled();
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
                throw AerospikeGraphAuthException.userNotFoundInParameters();
            }
            final String username = (String) params.get("name").get(0);
            final ROLE role = getRole(params.get("role").get(0), graph.getBaseGraph().GRAPH_ID);
            final Map allRoles = params.get("role").get(0) instanceof Map? (Map) params.get("role").get(0) : null;
            userClaims.set(new UserClaims(username, role, allRoles));
            adminStep = callStep;
        }

        // Add token.
        callSteps.forEach(callStep -> callStep.configure(RESERVED_USER_CONTEXT, userClaims.get()));

        // Remove admin step.
        if (adminStep != null) {
            traversal.removeStep(adminStep);
        }

        if (userClaims.get() == null) {
            throw AerospikeGraphAuthException.userNotFoundInParameters();
        } else {
            final ROLE role = userClaims.get().getRole();
            if (role == null) {
                throw AerospikeGraphAuthException.userDoesNotHaveValidRole();
            }
            // Admin steps are call steps. These have internal auth checks.
            if (hasMutateStep.get()) {
                if (!role.equals(ROLE.READ_WRITE) && !role.equals(ROLE.ADMIN)) {
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
                        LOG.info("[{}] - " + " Insufficient permissions to execute mutating step. Query: '{}'.", userClaims.get().getUsername(),
                                TraversalUtil.toStringScript(copy.asAdmin(), graph.getBaseGraph().REDACT_SCRIPT_LITERALS_ENABLED));
                    }
                    throw AerospikeGraphAuthException.userDoesNotHaveWriteAccess();
                }
            }
            if (!hasMutateStep.get()) {
                if (!role.equals(ROLE.READ) &&
                        !role.equals(ROLE.READ_WRITE) &&
                        !role.equals(ROLE.ADMIN)) {
                    throw AerospikeGraphAuthException.userDoesNotHaveReadAccess();
                }
            }
        }
        graph.setUser(userClaims.get().username);
    }

    @Override
    public void reset() {
        userClaims.remove();
        hasMutateStep.remove();
    }

    private static ROLE getRole(final Object claim, final String graphId) {
        if (claim == null) {
            return null;
        }

        if (claim instanceof String) {
            return ROLE.valueOf((String) claim);
        }

        final Object graphRole = ((Map) claim).get(graphId);
        // no role defined for this Graph
        if (graphRole == null) {
            return null;
        }
        return ROLE.valueOf((String) graphRole);
    }
}
