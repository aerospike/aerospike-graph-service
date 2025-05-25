package com.aerospike.firefly.io;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public abstract class FireflyCache {
    abstract public Record read(final Policy policy, final Key key);

    abstract public Record read(final WritePolicy policy, final Key key, final Operation[] operations);

    abstract public Record[] read(final Key[] keys, final BatchPolicy policy);

    abstract public Record[] read(final Key[] keys, final BatchPolicy policy, final Operation[] operations);

    abstract public void write(final WritePolicy writePolicy, final Key key, final Bin... bins);

    abstract public void remove(final Key key);

    abstract public void invalidate(final Key key);

    abstract public void insert(final Key key, final Record record);

    abstract public void invalidateAll();

    abstract public long getHitCount();

    abstract public long getMissCount();
}
