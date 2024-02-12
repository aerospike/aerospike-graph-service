package com.aerospike.firefly.process.call.sindex;

import com.aerospike.firefly.io.aerospike.admin.AdminContext;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.structure.service.Service.Type.Start;

public abstract class SindexServiceBase<I, R> implements Service.ServiceFactory<I, R>, Service<I, R>  {
    protected final FireflyGraph firefly;
    protected static final String ELEMENT_TYPE = "element_type";
    protected static final String PROPERTY_KEY = "property_key";

    public static void registerSindexServices(final FireflyGraph firefly) {
        firefly.getServiceRegistry().registerService(new SindexServiceCardinality(firefly));
        firefly.getServiceRegistry().registerService(new SindexServiceCreate(firefly));
        firefly.getServiceRegistry().registerService(new SindexServiceDrop(firefly));
        firefly.getServiceRegistry().registerService(new SindexServiceList(firefly));
        firefly.getServiceRegistry().registerService(new SindexServiceStatus(firefly));
    }

    public SindexServiceBase(final FireflyGraph firefly) {
        this.firefly = firefly;
    }

    @Override
    public String getName() {
        return "aerospike.graph.admin.index." + adminServiceName();
    }

    protected abstract String adminServiceName();

    @Override
    public Set<Service.Type> getSupportedTypes() {
        return Set.of(Service.Type.Start);
    }

    protected abstract Map<String, String> getParamDescription();

    @Override
    public Map<String, String> describeParams() {
        return getParamDescription();
    }

    @Override
    public Service<I, R> createService(final boolean isStart, final Map params) {
        if (!isStart) {
            throw new UnsupportedOperationException(Service.Exceptions.cannotUseMidTraversal);
        }
        return this;
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

    // Dummy for now.
    class EmptyAdminContext<V> implements AdminContext {
        @Override
        public V getContext() {
            return null;
        }
    }
}
