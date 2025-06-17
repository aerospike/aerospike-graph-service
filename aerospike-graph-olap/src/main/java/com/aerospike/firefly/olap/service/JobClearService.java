package com.aerospike.firefly.olap.service;

import com.aerospike.firefly.olap.process.packing.DistributedAerospikeConnection;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class JobClearService<I, R> implements Service.ServiceFactory<I, R>, Service<I, R> {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobClearService.class);

    @Override
    public Type getType() {
        return Type.Start;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Service.super.getRequirements();
    }

    // start
    @Override
    public CloseableIterator execute(final ServiceCallContext ctx, final Map params) {

        final DistributedAerospikeConnection db = (DistributedAerospikeConnection) params.get("db");
        db.removeAllJobs();

        LOGGER.warn("AGA job log cleared.");

        final List<String> result = List.of("AGA job log cleared.");
        return CloseableIterator.of(result.iterator());
    }

    @Override
    public String getName() {
        return "aerospike.graph.analytics.job.clearLog";
    }

    @Override
    public Set<Type> getSupportedTypes() {
        return Collections.singleton(Type.Start);
    }

    @Override
    public Service<I, R> createService(final boolean isStart, final Map params) {
        return this;
    }

    @Override
    public void close() {
        ServiceFactory.super.close();
    }
}
