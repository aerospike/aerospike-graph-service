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

package com.aerospike.firefly.runtime.metrics;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import io.prometheus.client.Collector;

import java.util.List;

public class FireflyMetricCollector extends Collector {
    private final String clusterName;
    private final FireflyGraph graph;

    public FireflyMetricCollector(final FireflyGraph graph) {
        this.clusterName = AerospikeConnection.InfoOps.getClusterName(graph.getBaseGraph());
        this.graph = graph;
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return List.of(
                new MetricFamilySamples("usage", Type.INFO, "Aerospike Graph Service Usage (vcpu-hours)",
                        List.of(new MetricFamilySamples.Sample(
                                "usage", List.of("usage"), List.of("aerospike_graph_usage"),
                                graph.getUsageStats().getTotalVcpuHours(graph.getUsageStats().readMetadata(), null)))),
                new MetricFamilySamples("cluster_name", Type.INFO, "Aerospike Cluster Name",
                        List.of(new MetricFamilySamples.Sample(
                                "cluster_name", List.of("cluster_name"), List.of(clusterName), 0.0))));
    }
}
