package com.aerospike.firefly.io.aerospike.admin.services;

import com.aerospike.firefly.runtime.tasks.FireflyGraphSummaryUpdater;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.structure.service.Service.Type.Start;

public abstract class AdminServiceFactoryBase<I, R> implements Service.ServiceFactory<I, R>, Service<I, R> {
    protected final FireflyGraph firefly;

    public AdminServiceFactoryBase(final FireflyGraph firefly) {
        this.firefly = firefly;
    }

    @Override
    public String getName() {
        return "admin." + adminServiceName();
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

    @Override
    public Type getType() {
        return Start;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Service.super.getRequirements();
    }

    @Override
    public void close() {
        ServiceFactory.super.close();
        Service.super.close();
    }
}