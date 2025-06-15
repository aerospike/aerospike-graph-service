package com.aerospike.firefly.olap.config;

// this parameters should be accessible without DistributedConfigHelper
public class QueryParameters {
    public static final String OLAP_PREFIX = "aerospike.graph.analytics.";

    // global flags
    public static final String ALLOW_UNFILTERED_ALGORITHM = OLAP_PREFIX + "unfiltered.algorithm.enabled";

    // per step flag
    public static final String REPARTITION = OLAP_PREFIX + "repartition";
}
