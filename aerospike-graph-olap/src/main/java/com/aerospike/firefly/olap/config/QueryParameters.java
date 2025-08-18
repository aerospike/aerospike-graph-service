package com.aerospike.firefly.olap.config;

import static com.aerospike.firefly.olap.config.DistributedConfigHelper.OLAP_PREFIX;

// this parameters should be accessible without DistributedConfigHelper
public class QueryParameters {
    // global flags
    public static final String ALLOW_UNFILTERED_ALGORITHM = OLAP_PREFIX + "unfiltered.algorithm.enabled";

    // per step flag
    public static final String REPARTITION = OLAP_PREFIX + "repartition";
}
