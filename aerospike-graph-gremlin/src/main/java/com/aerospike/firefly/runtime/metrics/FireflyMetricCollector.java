package com.aerospike.firefly.runtime.metrics;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import io.prometheus.client.Collector;

import java.util.List;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyMetricCollector extends Collector {
    private static final String PREFIX = "aerospike_graph_service_";
    private final String clusterName;

    public FireflyMetricCollector(final AerospikeConnection db) {
        this.clusterName = AerospikeConnection.InfoOps.getClusterName(db.getClient());
    }

    @Override
    public List<MetricFamilySamples> collect() {
        return List.of(
                new MetricFamilySamples("cluster_name", Type.INFO, "Aerospike Cluster Name",
                        List.of(new MetricFamilySamples.Sample(
                                "cluster_name", List.of("cluster_name"), List.of(clusterName), 0.0))));
    }
}
