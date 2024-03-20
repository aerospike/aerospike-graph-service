package com.aerospike.firefly.io.aerospike.admin;

import com.aerospike.firefly.process.call.sindex.SindexServiceBase;
import com.aerospike.firefly.process.call.sindex.SindexServiceCardinality;
import com.aerospike.firefly.process.call.sindex.SindexServiceCreate;
import com.aerospike.firefly.process.call.sindex.SindexServiceDrop;
import com.aerospike.firefly.process.call.sindex.SindexServiceList;
import com.aerospike.firefly.process.call.sindex.SindexServiceStatus;
import com.aerospike.firefly.structure.FireflyGraph;
import io.vertx.core.Handler;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.service.Service;

import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.structure.service.Service.Type.Start;

public abstract class AdminServiceRegistry<I, R> implements Service.ServiceFactory<I, R>, Service<I, R> {
    protected final FireflyGraph firefly;

    public AdminServiceRegistry(final FireflyGraph firefly) {
        this.firefly = firefly;
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
        return "aerospike.graph.admin." + getAdminNamespace() + "." + getAdminServiceName();
    }

    public String getPath() {
        return "/admin/" + getAdminNamespace() + "/" + getAdminServiceName();
    }

    public abstract Handler<RoutingContext> getHandler();

    // Not an Aerospike namespace. This is the namespace of the admin function, ex. 'index'.
    protected abstract String getAdminNamespace();
    protected abstract String getAdminServiceName();

    public static void registerAdminServices(final FireflyGraph firefly) {
        SindexServiceBase.registerSindexServices(firefly);
    }

    public static void appendHandlers(final Router router, final FireflyGraph graph) {
        SindexServiceBase.routerSindexServices(router, graph);
    }
}
