package com.aerospike.firefly.process.call;

import com.aerospike.firefly.bulkloader.BulkLoaderCallEntryPoint;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Map;
import java.util.Set;

public class FireflyBulkLoaderServiceFactory<I, R> implements Service.ServiceFactory<I, R>, Service<I, R> {

    @Override
    public String getName() {
        return "bulk-load";
    }

    @Override
    public Set<Type> getSupportedTypes() {
        return null;
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
        return null;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Service.super.getRequirements();
    }

    @Override
    public CloseableIterator<R> execute(final ServiceCallContext ctx, final Map params) {
        BulkLoad bulkLoad = new BulkLoaderCallEntryPoint();
        bulkLoad.perform(params);
        return CloseableIterator.empty();
    }

    @Override
    public void close() {
        ServiceFactory.super.close();
        Service.super.close();
    }


    public interface BulkLoad {
        public void perform(final Map parameters);
    }
}
