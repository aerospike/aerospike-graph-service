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
    protected final FireflyGraph firefly;
    protected static final String ELEMENT_TYPE = "element_type";
    protected static final String PROPERTY_KEY = "property_key";
    private static Set<SindexServiceBase> sindexServices;

    public static void registerSindexServices(final FireflyGraph firefly) {
        synchronized (SindexServiceBase.class) {
                Set.of(
                        new SindexServiceCardinality(firefly),
                        new SindexServiceCreate(firefly),
                        new SindexServiceDrop(firefly),
                        new SindexServiceList(firefly),
                        new SindexServiceStatus(firefly)
                ).forEach(firefly.getServiceRegistry()::registerService);
            System.out.println("REGISTERING SINDEX SERVICES");
        }
    }

    public static void routerSindexServices(final Router router, final FireflyGraph firefly) {
        synchronized (SindexServiceBase.class) {
                Set.of(
                        new SindexServiceCardinality(firefly),
                        new SindexServiceCreate(firefly),
                        new SindexServiceDrop(firefly),
                        new SindexServiceList(firefly),
                        new SindexServiceStatus(firefly)
                ).forEach(service -> router.route(service.getPath()).handler(service.getHandler()));
            System.out.println("ROUTING SINDEX SERVICES");
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

    @Override
    public Handler<RoutingContext> getHandler() {
        return routerContext -> {
            final Map<String, String> params = routerContext.queryParams().entries().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            if (!sanitize(params)) {
                routerContext.fail(400, new IllegalArgumentException(usage(params)));
                return;
            }
            final R result = execute(params);
            routerContext.response().end(result.toString());
        };
    }
}
