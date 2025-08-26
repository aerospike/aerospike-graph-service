package com.aerospike.firefly.process.call.config;

import com.aerospike.firefly.io.aerospike.admin.ServiceRegistryBase;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Set;

public class ConfigServiceRegistry extends ServiceRegistryBase {

    public ConfigServiceRegistry(final FireflyGraph graph) {
        services = Set.of(new ConfigServiceDumpDefaults<>(graph));
        services.forEach(graph.getServiceRegistry()::registerService);
    }
}
