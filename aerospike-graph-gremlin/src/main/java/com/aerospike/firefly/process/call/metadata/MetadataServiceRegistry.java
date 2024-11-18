package com.aerospike.firefly.process.call.metadata;

import com.aerospike.firefly.io.aerospike.admin.ServiceRegistryBase;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Set;

public class MetadataServiceRegistry extends ServiceRegistryBase {

    public MetadataServiceRegistry(final FireflyGraph graph) {
        services =  Set.of(
                new MetadataServiceUsage<>(graph),
                new MetadataServiceSummary<>(graph),
                new MetadataServiceConfig<>(graph),
                new MetadataServiceVersion<>(graph),
                new MetadataServiceUsageDeprecated<>(graph),
                new MetadataServiceSummaryDeprecated<>(graph)
        );

        services.forEach(graph.getServiceRegistry()::registerService);
    }
}
