package com.aerospike.firefly.io.aerospike.admin;

import com.aerospike.firefly.process.call.bulkload.BulkLoaderServiceBase;
import com.aerospike.firefly.process.call.metadata.MetadataServiceBase;
import com.aerospike.firefly.process.call.sindex.SindexServiceBase;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import io.vertx.core.Handler;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.http.auth.InvalidCredentialsException;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.structure.service.Service.Type.Start;

public abstract class AdminServiceRegistry<I, R> implements Service.ServiceFactory<I, R>, Service<I, R> {
    protected FireflyGraph graph;
    protected static final Logger LOGGER = LoggerFactory.getLogger(AdminServiceRegistry.class);

    public AdminServiceRegistry(final FireflyGraph graph) {
        this.graph = graph;
    }

    // Dummy for now.
    public class EmptyAdminContext<V> implements AdminContext {
        @Override
        public V getContext() {
            return null;
        }
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
        if (getAdminNamespace() != null) {
            return "/admin/" + getAdminNamespace() + "/" + getAdminServiceName();
        } else {
            return "/admin/" + getAdminServiceName();
        }
    }

    // Namespace of the graph project, ex. 'graphloader', or 'graph'.
    protected abstract String getGraphProjectNamespace();
    // Name of service, ex. 'create', 'drop', 'list', 'status'.
    protected abstract String getAdminNamespace();
    // Not an Aerospike namespace. This is the namespace of the admin function, ex. 'index'.
    protected abstract String getAdminServiceName();
    protected abstract String usage(Map params);
    protected abstract boolean sanitize(final Map params);
    protected abstract R execute(final Map params);
    protected abstract void auditLog(final Map params);

    public static void registerAdminServices(final FireflyGraph firefly) {
        SindexServiceBase.registerSindexServices(firefly);
        MetadataServiceBase.registerMetadataServices(firefly);
        BulkLoaderServiceBase.registerBulkLoadServices(firefly);
    }

    public static void appendHandlers(final Router router) {
        SindexServiceBase.routeSindexServices(router);
        MetadataServiceBase.routeMetadataServices(router);
        BulkLoaderServiceBase.routeBulkLoadServices(router);
    }

    @Override
    public CloseableIterator<R> execute(final ServiceCallContext ctx, final Map params) {
        // TODO: Implement this with RBAC.
        if (!validateAdminContext(ctx, params)) {
            throw new IllegalArgumentException("Invalid admin context.");
        }
        if (!sanitize(params)) {

            throw new IllegalArgumentException(usage(params));
        }
        auditLog(params);
        return FireflyCloseableIteratorUtils.of(execute(params));
    }

    private boolean validateAdminContext(final ServiceCallContext ctx, final Map params) {
        return true;
    }

    private static final int SUCCESS_CODE = 200;
    private static final int ERROR_CODE = 400;

    public Handler<RoutingContext> getHandler() {
        return routerContext -> {
            if (graph == null) {
                throw new IllegalStateException("Graph has not completed initialization.");
            }
            final Map<String, String> params = routerContext.queryParams().entries().stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            if (!sanitize(params)) {
                routerContext.fail(ERROR_CODE, new IllegalArgumentException(usage(params)));
                return;
            }
            final R result = execute(params);
            routerContext.response().setStatusCode(SUCCESS_CODE).putHeader("content-type", "text/html")
                    .end(String.valueOf(result));
        };
    }

    @Override
    public Set<Service.Type> getSupportedTypes() {
        return Set.of(Service.Type.Start);
    }
}
