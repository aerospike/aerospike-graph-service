package com.aerospike.firefly.process.call.bulkload;

import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Map;

public class BulkLoaderServiceCountErrorsDeprecated<I, R> extends BulkLoaderServiceCountErrors<I, R> {
    public BulkLoaderServiceCountErrorsDeprecated(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    public String getName() {
        return "get-bulk-load-error-count";
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.warn("Invoked deprecated API '" + getName() + "' for future use please see '" + super.getName() + "'.");
        super.auditLog(params);
    }
}
