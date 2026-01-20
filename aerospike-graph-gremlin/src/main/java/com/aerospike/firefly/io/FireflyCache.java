package com.aerospike.firefly.io;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;

import java.util.UUID;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public abstract class FireflyCache {
    protected final UUID uuid;

    public FireflyCache(final UUID uuid) {
        this.uuid = uuid;
    }

    abstract public Record read(final Policy policy, final Key key);

    abstract public Record read(final WritePolicy policy, final Key key, final Operation[] operations);

    abstract public Record[] read(final Key[] keys, final BatchPolicy policy, final Operation... operations);

    abstract public void write(final WritePolicy writePolicy, final Key key, final Bin... bins);

    abstract public void remove(final Key key);

    abstract public void invalidate(final Key key);

    abstract public void insert(final Key key, final Record record);

    abstract public void invalidateAll();

    abstract public long getHitCount();

    abstract public long getMissCount();

    /**
     * Returns the estimated number of entries in the cache.
     *
     * @return estimated entry count
     */
    abstract public long getEstimatedEntryCount();

    /**
     * Returns the weighted size of all entries in the cache.
     * The weight is calculated based on the number of edges and properties per record.
     *
     * @return weighted size
     */
    abstract public long getWeightedSize();

    /**
     * Returns an estimated memory usage of the cache in bytes.
     * This is an approximation based on the weighted size and average bytes per weight unit.
     *
     * @return estimated memory usage in bytes
     */
    abstract public long getEstimatedMemoryUsageBytes();

    @Override
    public String toString() {
        return "FireflyCache(" + uuid.toString() + ")";
    }
}
