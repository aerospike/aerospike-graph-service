package com.aerospike.firefly.process.call.query;

import com.aerospike.firefly.io.aerospike.admin.AdminServiceRegistry;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;
import io.vertx.ext.web.Router;

import java.util.Set;

public abstract class QueryServiceBase<I, R> extends AdminServiceRegistry<I, R> {

    public QueryServiceBase(final FireflyGraph firefly) {
        super(firefly);
    }

    @Override
    protected String getAdminNamespace() {
        return "query";
    }

    @Override
    protected String getGraphProjectNamespace() {
        return "graph";
    }

    protected static Set<QueryServiceBase> services;

    public static void registerQueryServices(final FireflyGraph graph) {
        synchronized (QueryServiceBase.class) {
            Set.of(new QueryServiceAbort<>(graph)).forEach(graph.getServiceRegistry()::registerService);

            // HTTP routing comes up before firefly. Need to latch firefly into the services.
            if (services != null) {
                services.forEach(s -> s.graph = graph);
            }
        }
    }

    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.ADMIN;
    }

    public static void routeQueryServices(final Router router) {
        synchronized (QueryServiceBase.class) {
            // Latch services so they can be updated later.
            if (services == null) {
                services = Set.of(new QueryServiceAbort<>(null));
                services.forEach(service -> router.route(service.getPath()).handler(service.getHandler()));
            }
        }
    }
}
