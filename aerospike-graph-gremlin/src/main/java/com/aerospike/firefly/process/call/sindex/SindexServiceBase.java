package com.aerospike.firefly.process.call.sindex;

import com.aerospike.firefly.io.aerospike.admin.AdminServiceRegistry;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import io.vertx.core.Handler;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class SindexServiceBase<I, R> extends AdminServiceRegistry<I, R> {
    protected FireflyGraph firefly;
    protected static final String ELEMENT_TYPE = "element_type";
    protected static final String PROPERTY_KEY = "property_key";
    protected static Set<SindexServiceBase> services;

    public static void registerSindexServices(final FireflyGraph firefly) {
        synchronized (SindexServiceBase.class) {
                Set.of(
                        new SindexServiceCardinality(firefly),
                        new SindexServiceCreate(firefly),
                        new SindexServiceDrop(firefly),
                        new SindexServiceList(firefly),
                        new SindexServiceStatus(firefly)
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
                        new SindexServiceCardinality(null),
                        new SindexServiceCreate(null),
                        new SindexServiceDrop(null),
                        new SindexServiceList(null),
                        new SindexServiceStatus(null));
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
    public Set<Service.Type> getSupportedTypes() {
        return Set.of(Service.Type.Start);
    }

    protected abstract Map<String, String> getParamDescription();

    @Override
    public Map<String, String> describeParams() {
        return getParamDescription();
    }

    protected abstract String usage(Map params);
    protected abstract boolean sanitize(final Map params);
    protected abstract R execute(final Map params);

    @Override
    public CloseableIterator<R> execute(final ServiceCallContext ctx, final Map params) {
        if (!sanitize(params)) {
            throw new IllegalArgumentException(usage(params));
        }
        return FireflyCloseableIteratorUtils.of(execute(params));
    }

    private static final int SINDEX_SUCCESS_CODE = 200;
    private static final int SINDEX_ERROR_CODE = 400;

    @Override
    public Handler<RoutingContext> getHandler() {
        return routerContext -> {
            if (firefly == null) {
                throw new IllegalStateException("Graph has not completed initialization.");
            }
            final Map<String, String> params = routerContext.queryParams().entries().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            if (!sanitize(params)) {
                routerContext.fail(SINDEX_ERROR_CODE, new IllegalArgumentException(usage(params)));
                return;
            }
            final R result = execute(params);
            routerContext.response().setStatusCode(SINDEX_SUCCESS_CODE).putHeader("content-type", "text/html")
                    .end(String.valueOf(result));
        };
    }
}
