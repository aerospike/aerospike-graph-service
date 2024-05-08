package com.aerospike.firefly.process.call.rbac;

import com.aerospike.firefly.io.aerospike.admin.AdminServiceRegistry;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;
import io.vertx.ext.web.Router;

import java.util.Set;

public abstract class JwtServiceBase<I, R> extends AdminServiceRegistry<I, R> {

    public JwtServiceBase(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminNamespace() {
        return "rbac-jwt";
    }

    @Override
    protected String getGraphProjectNamespace() {
        return "graph";
    }

    protected static Set<JwtServiceBase> services;

    public static void registerJwtServices(final FireflyGraph graph) {
        synchronized (JwtServiceBase.class) {
            Set.of(new JwtServiceIssueToken<>(graph)).forEach(graph.getServiceRegistry()::registerService);

            // HTTP routing comes up before firefly. Need to latch firefly into the services.
            if (services != null) {
                services.forEach(s -> s.graph = graph);
            }
        }
    }

    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.ADMIN;
    }

    public static void routeJwtServices(final Router router) {
        synchronized (JwtServiceBase.class) {
            // Latch services so they can be updated later.
            if (services == null) {
                services = Set.of(new JwtServiceIssueToken<>(null));
                services.forEach(service -> router.route(service.getPath()).handler(service.getHandler()));
            }
        }
    }
}