package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.security.JWTAuthenticator;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Deleting;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.Writing;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CallStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static com.aerospike.firefly.io.aerospike.admin.AdminServiceRegistry.RESERVED_USER_CONTEXT;
import static com.aerospike.firefly.security.JWTAuthorizer.RESERVED_CALL_STRING;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyAuthenticationStrategy extends FireflyStrategyBase {

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
        boolean hasMutateStep = false;
        boolean hasAdminStep = false;

        final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();
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
                } catch (NoSuchFieldException | IllegalAccessException ignore) {
                }

                if (RESERVED_CALL_STRING.equals(serviceName)) {
                    throw new RuntimeException("Error, authentication is disabled but credentials were provided.");
                }
            }
            return;
        }

        Step adminStep = null;
        JWTAuthenticator.JWTAuthenticatedUser jwtUser = null;
        for (final Step step : traversal.getSteps()) {
            if (step instanceof CallStep) {
                final CallStep callStep = (CallStep) step;
                String serviceName = null;
                // The serviceName is private, need to use reflection to get it so the compiler doesn't complain.
                try {
                    final Field serviceNameField = CallStep.class.getDeclaredField("serviceName");
                    serviceNameField.setAccessible(true);
                    serviceName = (String) serviceNameField.get(callStep);
                } catch (NoSuchFieldException | IllegalAccessException ignore) {
                }

                if (RESERVED_CALL_STRING.equals(serviceName)) {
                    try {
                        final Field parametersField = CallStep.class.getDeclaredField("parameters");
                        parametersField.setAccessible(true);
                        final Parameters parameters = (Parameters) parametersField.get(callStep);
                        final Map<Object, List<Object>> params = parameters.getRaw();
                        if (!params.containsKey("user")
                                || params.get("user").size() != 1 ||
                                !(params.get("user").get(0) instanceof JWTAuthenticator.JWTAuthenticatedUser)) {
                            throw new RuntimeException("User not found in parameters.");
                        }

                        jwtUser = (JWTAuthenticator.JWTAuthenticatedUser) params.get("user").get(0);
                        adminStep = step;
                    } catch (final IllegalAccessException | NoSuchFieldException e) {
                        throw new RuntimeException(e);
                    }

                } else {
                    callStep.configure(RESERVED_USER_CONTEXT, jwtUser);
                }
            } else if (step instanceof Mutating) {
                hasMutateStep = true;
            }
        }

        if (adminStep != null) {
            traversal.removeStep(adminStep);
        }

        if (jwtUser == null) {
            throw new RuntimeException("User not found in parameters.");
        } else {
            final UserContext.ROLE role = jwtUser.getRole();
            if (hasMutateStep) {
                if (!role.equals(UserContext.ROLE.WRITE) && !role.equals(UserContext.ROLE.ADMIN)) {
                    throw new RuntimeException("User does not have write access.");
                }
            }
            if (hasAdminStep) {
                if (!role.equals(UserContext.ROLE.ADMIN)) {
                    throw new RuntimeException("User does not have admin access.");
                }
            }
            if (!hasMutateStep && !hasAdminStep) {
                if (!role.equals(UserContext.ROLE.READ) &&
                        !role.equals(UserContext.ROLE.WRITE) &&
                        !role.equals(UserContext.ROLE.ADMIN)) {
                    throw new RuntimeException("User does not have read access.");
                }
            }
        }
    }
}
