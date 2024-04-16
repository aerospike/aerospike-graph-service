package com.aerospike.firefly.process.call.bulkload;

import com.aerospike.firefly.io.aerospike.admin.AdminServiceRegistry;
import com.aerospike.firefly.security.UserContext;
import com.aerospike.firefly.structure.FireflyGraph;
import io.vertx.ext.web.Router;

import java.util.Set;

public abstract class BulkLoaderServiceBase<I, R> extends AdminServiceRegistry<I, R> {

    public BulkLoaderServiceBase(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminNamespace() {
        return "bulk-load";
    }

    @Override
    protected String getGraphProjectNamespace() {
        return "graphloader";
    }

    protected static Set<BulkLoaderServiceBase> services;

    public static void registerBulkLoadServices(final FireflyGraph graph) {
        synchronized (BulkLoaderServiceBase.class) {
            Set.of(
                    new BulkLoaderServiceLoad<>(graph),
                    new BulkLoaderServiceErrors<>(graph),
                    new BulkLoaderServiceCountErrors<>(graph)
            ).forEach(graph.getServiceRegistry()::registerService);

            // HTTP routing comes up before firefly. Need to latch firefly into the services.
            if (services != null) {
                services.forEach(s -> s.graph = graph);
            }
        }
    }

    protected UserContext.ROLE getRequiredRole() {
        return UserContext.ROLE.READ_WRITE;
    }

    public static void routeBulkLoadServices(final Router router) {
        synchronized (BulkLoaderServiceBase.class) {
            // Latch services so they can be updated later.
            if (services == null) {
                services = Set.of(
                        new BulkLoaderServiceLoad<>(null),
                        new BulkLoaderServiceErrors<>(null),
                        new BulkLoaderServiceCountErrors<>(null));
                services.forEach(service -> router.route(service.getPath()).handler(service.getHandler()));
            }
        }
    }
}
