package com.aerospike.firefly.io.impl;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyCache;
import com.google.common.base.Optional;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;

import java.util.concurrent.atomic.AtomicLong;


/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class SubgraphCache implements FireflyCache {
    public static final WritePolicy sendKeyWritePolicy = new WritePolicy();
    private final AtomicLong hitCounter = new AtomicLong(0);
    private final AtomicLong missCounter = new AtomicLong(0);

    static {
        sendKeyWritePolicy.sendKey = true;
    }

    private final CacheLoader<Key, Optional<Record>> loader;

    //Non-loading cache does not cache new results
    private final Cache<Key, Optional<Record>> cache;
    private final AerospikeConnection db;


    public SubgraphCache(AerospikeConnection db) {
        this.db = db;
        loader = new CacheLoader<Key, Optional<Record>>() {
            @Override
            public Optional<Record> load(Key key) {
                Optional<Record> result = Optional.fromNullable(db.getClient().get(new Policy(), key));
                return result;
            }
        };
        cache = CacheBuilder.newBuilder().build();
    }

    @Override
    public Record read(Key key) {
        Optional<Record> or = cache.getIfPresent(key);
        if (or != null && or.isPresent()) {
            hitCounter.incrementAndGet();
            return or.get();
        } else {
            missCounter.incrementAndGet();
            cache.invalidate(key);
            return db.getClient().get(null, key);
        }
    }

    @Override
    public void write(Key key, Bin... bins) {
        cache.invalidate(key);
        db.getClient().put(sendKeyWritePolicy, key, bins);
    }

    public void remove(Key key) {
        try {
            cache.invalidate(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        db.getClient().delete(sendKeyWritePolicy, key);
    }

    @Override
    public void invalidate(Key key) {
        cache.invalidate(key);
    }

    @Override
    public void insert(Key key, Record record) {
        cache.put(key, Optional.of(record));
    }
    public long getHitCount(){return hitCounter.get();}
    public long getMissCount(){return missCounter.get();}
}
