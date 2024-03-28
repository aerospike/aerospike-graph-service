package com.aerospike.firefly.process.call.sindex;

import com.aerospike.firefly.io.aerospike.admin.AdminServiceRegistry;
import com.aerospike.firefly.structure.FireflyGraph;
import io.vertx.ext.web.Router;

import java.util.Set;

public abstract class SindexServiceBase<I, R> extends AdminServiceRegistry<I, R> {
    protected FireflyGraph firefly;
    protected static final String ELEMENT_TYPE = "element_type";
    protected static final String PROPERTY_KEY = "property_key";
    protected static Set<SindexServiceBase> services;

    public static void registerSindexServices(final FireflyGraph firefly) {
        synchronized (SindexServiceBase.class) {
                Set.of(
                        new SindexServiceCardinality<>(firefly),
                        new SindexServiceCreate<>(firefly),
                        new SindexServiceDrop<>(firefly),
                        new SindexServiceList<>(firefly),
                        new SindexServiceStatus<>(firefly)
                ).forEach(firefly.getServiceRegistry()::registerService);

            // HTTP routing comes up before firefly. Need to latch firefly into the services.
            if (services != null) {
                services.forEach(s -> s.firefly = firefly);
            }
        }
    }

    public static void routeSindexServices(final Router router) {
        synchronized (SindexServiceBase.class) {
            // Latch services so they can be updated later.
            if (services == null) {
                services = Set.of(
                        new SindexServiceCardinality<>(null),
                        new SindexServiceCreate<>(null),
                        new SindexServiceDrop<>(null),
                        new SindexServiceList<>(null),
                        new SindexServiceStatus<>(null));
                services.forEach(service -> router.route(service.getPath()).handler(service.getHandler()));
            }
        }
    }

    public SindexServiceBase(final FireflyGraph firefly) {
        super(firefly);
        this.firefly = firefly;
    }

    @Override
    protected String getAdminNamespace() {
        return "index";
    }

    @Override
    protected String getGraphProjectNamespace() {
        return "graph";
    }
}
