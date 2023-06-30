package com.aerospike.firefly.util;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.async.EventLoops;
import org.apache.commons.configuration2.Configuration;

public interface AerospikeClientProvider {
    public AerospikeClient getAerospikeClient(Configuration config);

    public EventLoops getEventLoops(Configuration config);
}
