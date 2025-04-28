package com.aerospike.firefly.process.call.bulkload;

import com.aerospike.firefly.io.aerospike.admin.ServiceRegistryBase;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Set;

public class BulkLoaderServiceRegistry extends ServiceRegistryBase {

    public BulkLoaderServiceRegistry(final FireflyGraph graph) {
        // Check if com.aerospike.firefly.bulkloader.SparkBulkLoaderMain exists. Only load if it does.
        if (!bulkLoaderExists()) {
            services = Set.of();
            return;
        }
        services = Set.of(
                new BulkLoaderServiceLoad<>(graph),
                new BulkLoaderServiceErrors<>(graph),
                new BulkLoaderServiceCountErrors<>(graph),
                new BulkLoaderServiceLoadDeprecated<>(graph),
                new BulkLoaderServiceErrorsDeprecated<>(graph),
                new BulkLoaderServiceCountErrorsDeprecated<>(graph),
                new BulkLoaderServiceStatus<>(graph));

        services.forEach(graph.getServiceRegistry()::registerService);
    }

    private boolean bulkLoaderExists() {
        try {
            Class.forName("com.aerospike.firefly.bulkloader.SparkBulkLoaderMain");
            return true;
        } catch (final ClassNotFoundException e) {
            return false;
        }
    }
}
