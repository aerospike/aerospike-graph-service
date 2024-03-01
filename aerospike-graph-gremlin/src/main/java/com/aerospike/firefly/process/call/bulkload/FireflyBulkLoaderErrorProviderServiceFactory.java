package com.aerospike.firefly.process.call.bulkload;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.structure.service.Service.Type.Start;

public class FireflyBulkLoaderErrorProviderServiceFactory<I, R> implements Service.ServiceFactory<I, R>, Service<I, R> {
    private static final String KEY = "type";
    private static final String DUPLICATE_VID = "duplicate-vertex-ids";
    private static final String BAD_ENTRY = "bad-entries";
    private static final String BAD_EDGE = "bad-edges";
    private static final Set<String> VALUES = Set.of(DUPLICATE_VID, BAD_ENTRY, BAD_EDGE);

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
        return "get-bulk-load-errors";
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
        if (params.size() != 1 || !params.containsKey(KEY) || !VALUES.contains(params.get(KEY))) {
            final StringBuilder sb = new StringBuilder();
            sb.append("\"get-bulk-load-errors\" must be invoked with parameter \"" + KEY
                    + "\" and only one of the following values: ");
            for (final String value : VALUES) {
                sb.append("\n").append(value);
            }
            throw new RuntimeException(sb.toString());
        }
        final FireflyGraph graph = (FireflyGraph) ctx.getTraversal().getGraph().get();
        if (params.get(KEY).equals(DUPLICATE_VID)) {
            return (CloseableIterator<R>) graph.readDuplicateVertexIdErrors();
        } else if (params.get(KEY).equals(BAD_ENTRY)) {
            return (CloseableIterator<R>) graph.readBadEntryErrors();
        } else if (params.get(KEY).equals(BAD_EDGE)) {
            return (CloseableIterator<R>) graph.readBadEdgeErrors();
        } else {
            // This should never happen since it's already safety checked above.
            throw new IllegalStateException("Invalid \"" + KEY + "\" detected for \"get-bulk-load-errors\"");
        }
    }
}
