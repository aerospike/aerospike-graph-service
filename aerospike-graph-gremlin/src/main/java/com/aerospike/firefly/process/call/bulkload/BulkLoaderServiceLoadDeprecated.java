package com.aerospike.firefly.process.call.bulkload;

import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Map;

public class BulkLoaderServiceLoadDeprecated<I, R> extends BulkLoaderServiceLoad<I, R> {
    public BulkLoaderServiceLoadDeprecated(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    public String getName() {
        return "bulk-load";
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.warn("Invoked deprecated API '" + getName() + "' for future use please see '" + super.getName() + "'.");
        super.auditLog(params);
    }
}
