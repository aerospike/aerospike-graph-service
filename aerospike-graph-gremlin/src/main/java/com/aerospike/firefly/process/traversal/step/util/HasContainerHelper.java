package com.aerospike.firefly.process.traversal.step.util;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class HasContainerHelper {
    public static List<HasContainer> convert(final List<HasContainer> containers, final FireflyGraph graph) {
        if (containers == null) {
            return null;
        }

        final List<HasContainer> result = new ArrayList<>(containers.size());
        // translate predicates to use FireflyId
        containers.forEach(c -> {
            final Object value = c.getPredicate().getValue();
            final Object valueWithFireflyIds = value instanceof List
                    ? ((List<Object>) value).stream().map(v -> graph.getIdFactory().createVertexId(v)).collect(Collectors.toList())
                    : graph.getIdFactory().createVertexId(value);
            final P predicate = new P(c.getPredicate().getBiPredicate(), valueWithFireflyIds);
            result.add(new HasContainer(c.getKey(), predicate));
        });

        return result;
    }
}
