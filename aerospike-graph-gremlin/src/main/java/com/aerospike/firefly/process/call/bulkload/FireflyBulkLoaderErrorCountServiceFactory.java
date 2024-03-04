package com.aerospike.firefly.process.call.bulkload;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.structure.service.Service.Type.Start;

public class FireflyBulkLoaderErrorCountServiceFactory<I, R> implements Service.ServiceFactory<I, R>, Service<I, R> {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyBulkLoaderErrorCountServiceFactory.class);

    @Override
    public Type getType() {
        return Start;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Service.super.getRequirements();
    }

    @Override
    public String getName() {
        return "get-bulk-load-error-count";
    }

    @Override
    public Set<Type> getSupportedTypes() {
        return Set.of(Start);
    }

    @Override
    public Service<I, R> createService(boolean isStart, Map params) {
        if (!isStart) {
            throw new UnsupportedOperationException(Service.Exceptions.cannotUseMidTraversal);
        }
        return this;
    }

    @Override
    public void close() {
        ServiceFactory.super.close();
        Service.super.close();
    }

    @Override
    public CloseableIterator<R> execute(final ServiceCallContext ctx, final Map params) {
        if (!params.isEmpty()) {
            LOG.warn("get-bulk-load-error-count does not support any parameters.");
        }

        final Map<String, Long> errorCounts = new HashMap<>();
        final FireflyGraph graph = (FireflyGraph) ctx.getTraversal().getGraph().get();
        final AerospikeConnection db = graph.getBaseGraph();
        final long badEntryCount = db.incrementAndGetBadEntryCount(0);
        final long duplicateVertexIdCount = db.incrementAndGetDuplicateVertexIdCount(0);
        final long badEdgeCount = db.incrementAndGetBadEdgeCount(0);
        errorCounts.put("duplicate-vertex-id-count", duplicateVertexIdCount);
        errorCounts.put("bad-edge-count", badEdgeCount);
        errorCounts.put("bad-entry-count", badEntryCount);
        return FireflyCloseableIteratorUtils.of((R) errorCounts);
    }
}
