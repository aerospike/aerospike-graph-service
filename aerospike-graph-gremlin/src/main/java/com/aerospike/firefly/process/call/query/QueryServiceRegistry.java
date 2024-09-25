package com.aerospike.firefly.process.call.query;

import com.aerospike.firefly.io.aerospike.admin.ServiceRegistryBase;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Set;

public class QueryServiceRegistry extends ServiceRegistryBase {

    public QueryServiceRegistry(final FireflyGraph graph) {
        services = Set.of(new QueryServiceAbort<>(graph));
        services.forEach(graph.getServiceRegistry()::registerService);
    }
}
