package com.aerospike.firefly.process.call.metadata;

import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Map;

public class MetadataServiceUsageDeprecated<I, R> extends MetadataServiceUsage<I, R> {
    public MetadataServiceUsageDeprecated(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    public String getName() {
        return "usage-stats";
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.warn("Invoked deprecated API '" + getName() + "' for future use please see '" + super.getName() + "'.");
        super.auditLog(params);
    }
}
