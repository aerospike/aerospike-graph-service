package com.aerospike.firefly.runtime.metrics;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.runtime.tasks.FireflyUsageStats;
import io.prometheus.client.Collector;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyMetricCollector extends Collector {
    private final String clusterName;

    public FireflyMetricCollector(final AerospikeConnection db) {
        this.clusterName = AerospikeConnection.InfoOps.getClusterName(db.getClient());
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return List.of(
                new MetricFamilySamples("usage", Type.INFO, "Aerospike Graph Service Usage (vcpu-hours)",
                        List.of(new MetricFamilySamples.Sample(
                                "usage", List.of("usage"), List.of("aerospike_graph_usage"),
                                FireflyUsageStats.getTotalVcpuHours( FireflyUsageStats.readMetadata(), null)))),
                new MetricFamilySamples("cluster_name", Type.INFO, "Aerospike Cluster Name",
                        List.of(new MetricFamilySamples.Sample(
                                "cluster_name", List.of("cluster_name"), List.of(clusterName), 0.0))));
    }
}
