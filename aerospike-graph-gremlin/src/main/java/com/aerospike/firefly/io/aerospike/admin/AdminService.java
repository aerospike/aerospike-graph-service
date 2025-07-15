package com.aerospike.firefly.io.aerospike.admin;

import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyAuthenticationStrategy.UserClaims;
import com.aerospike.firefly.security.JWTAuthenticator;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.util.exceptions.AerospikeGraphAuthException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.ext.web.RoutingContext;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.server.auth.AuthenticatedUser;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aerospike.firefly.security.UserContext.ROLE;
import static org.apache.tinkerpop.gremlin.structure.service.Service.Type.Start;

public abstract class AdminService<I, R> implements Service.ServiceFactory<I, R>, Service<I, R> {
    protected FireflyGraph graph;
    private String user = null;
    protected static final Logger LOGGER = LoggerFactory.getLogger(AdminService.class);
    public static final String RESERVED_USER_CONTEXT = "aerospike.graph.admin.reserved.user.context";

    public AdminService(final FireflyGraph graph) {
        this.graph = graph;
    }

    @Override
    public Service<I, R> createService(final boolean isStart, final Map params) {
        if (!isStart) {
            throw new UnsupportedOperationException(Service.Exceptions.cannotUseMidTraversal);
        }
        return this;
    }

    @Override
    public Service.Type getType() {
        return Start;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Service.super.getRequirements();
    }

    @Override
    public void close() {
        Service.ServiceFactory.super.close();
        Service.super.close();
    }


    @Override
    public String getName() {
        if (getAdminNamespace() != null) {
            return "aerospike." + getGraphProjectNamespace() + ".admin." + getAdminNamespace() + "." + getAdminServiceName();
        } else {
            return "aerospike." + getGraphProjectNamespace() + ".admin." + getAdminServiceName();
        }
    }

    public String getPath() {
        final String graphPrefix = "/" + graph.getBaseGraph().GRAPH_ID;

        if (getAdminNamespace() != null) {
            return graphPrefix + "/admin/" + getAdminNamespace() + "/" + getAdminServiceName();
        } else {
            return graphPrefix + "/admin/" + getAdminServiceName();
        }
    }

    // Namespace of the graph project, ex. 'graphloader', or 'graph'.
    protected abstract String getGraphProjectNamespace();
    // Name of service, ex. 'create', 'drop', 'list', 'status'.
    protected abstract String getAdminNamespace();
    // Not an Aerospike namespace. This is the namespace of the admin function, ex. 'index'.
    protected abstract String getAdminServiceName();
    protected abstract String usage(final Map params);
    protected abstract boolean sanitize(final Map params);
    protected abstract R execute(final Map params);
    protected abstract void auditLog(final Map params);
    protected abstract ROLE getRequiredRole();

    @Override
    public CloseableIterator<R> execute(final ServiceCallContext ctx, final Map params) {
        if (!validateAdminContext(ctx, params)) {
            LOGGER.info("[{}] - {} - Insufficient permissions to run service.", getUser(), getName());
            throw new IllegalArgumentException("Insufficient permissions for '" + getName() + "'.");
        }
        if (!sanitize(params)) {
            throw new IllegalArgumentException(usage(params));
        }
        auditLog(params);
        return FireflyCloseableIteratorUtils.of(execute(params));
    }

    // service specific additional permissions validation
    protected boolean isValidPermissions(final Map params, final UserClaims userContext) {
        return true;
    }

    private boolean validateAdminContext(final ServiceCallContext ctx, final Map params) {
        if (!graph.getBaseGraph().AUTHENTICATION_ENABLED) {
            return true;
        }

        final UserClaims userContext = (UserClaims) params.remove(RESERVED_USER_CONTEXT);
        if (userContext == null) {
            // This should never happen.
            throw AerospikeGraphAuthException.invalidUserContext();
        }
        user = userContext.getUsername();

        final ROLE role = userContext.getRole(graph.getBaseGraph().GRAPH_ID);
        if (role == null) {
            // This can happen.
            throw AerospikeGraphAuthException.userDoesNotHaveValidRole();
        }
        if (!isValidPermissions(params, userContext)) {
            return false;
        }

        return isRoleHigher(getRequiredRole(), role);
    }

    protected boolean isRoleHigher(final ROLE requested, final ROLE available) {
        if (requested.equals(ROLE.ADMIN)) {
            return available.equals(ROLE.ADMIN);
        } else if (requested.equals(ROLE.READ_WRITE)) {
            return available.equals(ROLE.ADMIN) ||
                    available.equals(ROLE.READ_WRITE);
        } else {
            return available.equals(ROLE.ADMIN) ||
                    available.equals(ROLE.READ_WRITE) ||
                    available.equals(ROLE.READ);
        }
    }

    protected String getUser() {
        return user == null ? "anonymous" : user;
    }

    private static final int SUCCESS_CODE = 200;
    private static final int ERROR_CODE = 400;
    private static final int UNAUTHORIZED_CODE = 401;

    public Handler<RoutingContext> getHandler() {
        return routerContext -> {
            if (graph == null) {
                throw new IllegalStateException("Graph has not completed initialization.");
            }

            if (graph.getBaseGraph().AUTHENTICATION_ENABLED) {
                final MultiMap map = routerContext.request().headers();
                if (!map.contains("Authorization")) {
                    routerContext.fail(UNAUTHORIZED_CODE, new IllegalArgumentException("Authorization header is missing."));
                    return;
                }
                final String authToken = map.get("Authorization");
                if (!authToken.startsWith("Bearer ")) {
                    routerContext.fail(UNAUTHORIZED_CODE, new IllegalArgumentException("Authorization header is missing 'Bearer ' prefix."));
                    return;
                }
                final String token = authToken.substring("Bearer ".length());
                try {
                    final JWTAuthenticator authenticator = JWTAuthenticator.getInstance();
                    if (authenticator == null) {
                        // Should never happen.
                        throw new IllegalStateException("Authentication is not enabled or has not completed initialization.");
                    }

                    final AuthenticatedUser authenticatedUser = authenticator.authenticate(token);
                    if (authenticatedUser == null) {
                        // Should never happen.
                        routerContext.fail(UNAUTHORIZED_CODE, new IllegalArgumentException("Failed to authenticate user."));
                        return;
                    }

                    final JWTAuthenticator.JWTAuthenticatedUser jwtUser = (JWTAuthenticator.JWTAuthenticatedUser) authenticatedUser;
                    final ROLE role = jwtUser.getRole(graph.getBaseGraph().GRAPH_ID);
                    final ROLE requiredRole = getRequiredRole();

                    if (role == null) {
                        // Should never happen.
                        routerContext.fail(UNAUTHORIZED_CODE, new IllegalArgumentException("User does not have a valid role."));
                        return;
                    }

                    if (!isRoleHigher(requiredRole, role)) {
                        routerContext.fail(UNAUTHORIZED_CODE, new IllegalArgumentException("Insufficient permissions to perform operation."));
                        return;
                    }
                } catch (final org.apache.tinkerpop.gremlin.server.auth.AuthenticationException e) { // Full name b/c we use other AuthenticationException in this file.
                    routerContext.fail(UNAUTHORIZED_CODE, e);
                    return;
                }
            }

            final Map<String, String> params = routerContext.queryParams().entries().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            if (!sanitize(params)) {
                routerContext.fail(ERROR_CODE, new IllegalArgumentException(usage(params)));
                return;
            }

            try {
                final R result = execute(params);
                final ObjectWriter objectWriter = new ObjectMapper().writer().withDefaultPrettyPrinter();
                routerContext.response().setStatusCode(SUCCESS_CODE).putHeader("content-type", "application/json")
                        .end(objectWriter.writeValueAsString(result));
            } catch (final Exception e) {
                routerContext.fail(ERROR_CODE, e);
            }
        };
    }

    @Override
    public Set<Service.Type> getSupportedTypes() {
        return Set.of(Service.Type.Start);
    }

    public boolean needRouting() {
        return true;
    }
}
