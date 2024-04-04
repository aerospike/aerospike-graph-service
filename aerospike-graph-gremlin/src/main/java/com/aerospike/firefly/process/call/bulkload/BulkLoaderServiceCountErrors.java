package com.aerospike.firefly.process.call.bulkload;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.HashMap;
import java.util.Map;

public class BulkLoaderServiceCountErrors<I, R> extends BulkLoaderServiceBase<I, R> {

    public BulkLoaderServiceCountErrors(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminServiceName() {
        return "error-count";
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to '%s'.\n" +
                "\tExpected no arguments.\n" +
                "\tProvided arguments: '%s'.\n" +
                "\tExample of correct usage:\n" +
                "\t\tg.call(\"%s\").next();\n",
                getName(), params, getName());
    }

    @Override
    protected boolean sanitize(final Map params) {
        return params.isEmpty();
    }

    @Override
    protected R execute(final Map params) {
        final Map<String, Long> errorCounts = new HashMap<>();
        final AerospikeConnection db = graph.getBaseGraph();
        final long badEntryCount = db.incrementAndGetBadEntryCount(0);
        final long duplicateVertexIdCount = db.incrementAndGetDuplicateVertexIdCount(0);
        final long badEdgeCount = db.incrementAndGetBadEdgeCount(0);
        errorCounts.put("duplicate-vertex-id-count", duplicateVertexIdCount);
        errorCounts.put("bad-edge-count", badEdgeCount);
        errorCounts.put("bad-entry-count", badEntryCount);
        return (R) errorCounts;
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info(getName() + " Get bulk load error count.");
    }
}
