package com.aerospike.firefly.process.call.bulkload;

import com.aerospike.firefly.io.aerospike.admin.ServiceRegistryBase;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Set;

public class BulkLoaderServiceRegistry extends ServiceRegistryBase {

    public BulkLoaderServiceRegistry(final FireflyGraph graph) {
        services = Set.of(
                new BulkLoaderServiceLoad<>(graph),
                new BulkLoaderServiceErrors<>(graph),
                new BulkLoaderServiceCountErrors<>(graph),
                new BulkLoaderServiceLoadDeprecated<>(graph),
                new BulkLoaderServiceErrorsDeprecated<>(graph),
                new BulkLoaderServiceCountErrorsDeprecated<>(graph));

        services.forEach(graph.getServiceRegistry()::registerService);
    }
}
