package com.aerospike.firefly.io;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.WritePolicy;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public interface FireflyCache {
    Record read(Key key);
    void write(WritePolicy writePolicy, Key key, Bin... bins);
    void remove(Key key);

    void invalidate(Key key);
    void insert(Key key , Record record);
}
