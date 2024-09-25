package com.aerospike.firefly.process.call.sindex;

import com.aerospike.firefly.io.aerospike.admin.ServiceRegistryBase;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Set;

public class SindexServiceRegistry extends ServiceRegistryBase {

    public SindexServiceRegistry(final FireflyGraph graph) {
        services = Set.of(
                new SindexServiceCardinality<>(graph),
                new SindexServiceCreate<>(graph),
                new SindexServiceDrop<>(graph),
                new SindexServiceList<>(graph),
                new SindexServiceStatus<>(graph));

        services.forEach(graph.getServiceRegistry()::registerService);
    }
}
