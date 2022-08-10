package com.aerospike.firefly.io.impl;

import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.Cache;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.concurrent.ExecutionException;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class GuavaCache implements Cache {
    private final CacheLoader<Key, Optional<Record>> loader;
    private final LoadingCache<Key, Optional<Record>> cache;
    private final AerospikeConnection db;

    public GuavaCache(AerospikeConnection db) {
        this.db = db;
        loader = new CacheLoader<Key, Optional<Record>>() {
            @Override
            public Optional<Record> load(Key key) {
                Optional<Record> result = Optional.fromNullable(db.getClient().get(new Policy(), key));
                return result;
            }
        };
        cache = CacheBuilder.newBuilder().build(loader);
    }

    @Override
    public Record read(Key key) {
        try {
            Optional<Record> or = cache.get(key);
            if (or.isPresent()) {
                return or.get();
            } else {
                cache.invalidate(key);
                return null;
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void write(Key key, Bin... bins) {
        cache.invalidate(key);
        db.getClient().put(new WritePolicy(), key, bins);
    }

    public void remove(Key key) {
        try {
            cache.invalidate(key);
        } catch (Exception e) {
            e.printStackTrace();
        }
        db.getClient().delete(new WritePolicy(), key);
    }

    @Override
    public void invalidate(Key key) {
        cache.invalidate(key);
    }
}
