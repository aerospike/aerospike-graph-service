package com.aerospike.firefly.process.call.expressionindex;

import com.aerospike.firefly.io.aerospike.admin.ServiceRegistryBase;
import com.aerospike.firefly.structure.FireflyGraph;

import java.util.Set;

public class ExpressionIndexServiceRegistry extends ServiceRegistryBase {

    public ExpressionIndexServiceRegistry(final FireflyGraph graph) {
        services = Set.of(
                new ExpressionIndexServiceCreate<>(graph),
                new ExpressionIndexServiceDrop<>(graph),
                new ExpressionIndexServiceList<>(graph),
                new ExpressionIndexServiceStatus<>(graph));

        services.forEach(graph.getServiceRegistry()::registerService);
    }
}
