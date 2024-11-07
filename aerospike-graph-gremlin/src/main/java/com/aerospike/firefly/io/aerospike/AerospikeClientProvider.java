package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.async.EventLoops;
import org.apache.commons.configuration2.Configuration;

public interface AerospikeClientProvider {
    AerospikeClient getAerospikeClient(Configuration config);

    EventLoops getEventLoops(Configuration config);
}
