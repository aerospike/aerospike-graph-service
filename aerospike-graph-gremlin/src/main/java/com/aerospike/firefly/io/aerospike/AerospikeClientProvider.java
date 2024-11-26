package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.async.EventLoops;
import org.apache.commons.configuration2.Configuration;

import java.util.concurrent.ExecutorService;

public interface AerospikeClientProvider {
    AerospikeClient getAerospikeClient(Configuration config);
    EventLoops getEventLoops(Configuration config);
    ExecutorService getThreadedExecutorService(Configuration config);
}
