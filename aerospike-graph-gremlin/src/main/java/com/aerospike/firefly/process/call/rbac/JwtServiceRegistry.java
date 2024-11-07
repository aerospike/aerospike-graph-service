package com.aerospike.firefly.process.call.rbac;

import com.aerospike.firefly.io.aerospike.admin.ServiceRegistryBase;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Set;

public class JwtServiceRegistry extends ServiceRegistryBase {

    public JwtServiceRegistry(final FireflyGraph graph) {
        services = Set.of(new JwtServiceIssueToken<>(graph));

        services.forEach(graph.getServiceRegistry()::registerService);
    }
}
