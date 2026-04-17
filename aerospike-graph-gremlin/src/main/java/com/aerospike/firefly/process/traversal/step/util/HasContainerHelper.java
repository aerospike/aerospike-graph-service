/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.process.traversal.step.util;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
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

    public static List<HasContainer> getVertexFilter(final FireflyGraph graph) {
        return getVertexFilter(graph.graphComputerView.getGraphFilter());
    }

    public static List<HasContainer> getVertexFilter(final GraphFilter graphFilter) {
        if (graphFilter == null || graphFilter.getVertexFilter() == null || graphFilter.getVertexFilter().getSteps().isEmpty()) {
            return null;
        }

        // just use 1st HasStep for now
        if (graphFilter.getVertexFilter().getSteps().get(0) instanceof HasStep) {
            final HasStep hasStep = (HasStep) graphFilter.getVertexFilter().getSteps().get(0);
            return hasStep.getHasContainers();
        }

        return null;
    }

    public static List<HasContainer> getVertexFilter(final FireflyGraph graph, final List<HasContainer> additionalHasContainers) {
        if (graph.graphComputerView == null) {
            return additionalHasContainers;
        }

        final List<HasContainer> vertexFilter = getVertexFilter(graph.graphComputerView.getGraphFilter());
        if (vertexFilter == null) {
            return additionalHasContainers;
        }
        if (additionalHasContainers == null) {
            return vertexFilter;
        }

        // both arrays are read-only - create a new combined list
        final List<HasContainer> combined = new ArrayList<>(vertexFilter.size() + additionalHasContainers.size());
        combined.addAll(vertexFilter);
        combined.addAll(additionalHasContainers);
        return combined;
    }
}
