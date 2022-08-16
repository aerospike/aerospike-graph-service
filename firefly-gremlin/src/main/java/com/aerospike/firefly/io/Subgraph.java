package com.aerospike.firefly.io;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.firefly.io.impl.SubgraphCache;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.IntStream;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class Subgraph {
    SubgraphCache cache;
    public void updateSubgraphCache(Key[] key, Record[] record) {
        IntStream.range(0, key.length).forEach(i -> {
            cache.insert(key[i], record[i]);
        });
    }

    /**
     * Starting from egoId, collect all the Records associated with
     * the EgoNetwork of egoId, and the EgoNetworks of all vertices adjacent
     * to egoId, put them in the cache, and return the list of Keys associated with them.
     * @param graph
     * @param egoId
     */
    public Key[] primeSubgraphCache(FireflyGraph graph, Object egoId) {
        ConcurrentLinkedQueue<Key> cachedKeys = new ConcurrentLinkedQueue<>();

        EgoNetwork.create(FireflyId.of(FireflyVertex.class, egoId), graph)
                .vertexRecords
                .parallelStream()
                .forEach(kr -> {
                    EgoNetwork.create(FireflyId.of(FireflyVertex.class, kr.key.userKey), graph)
                            .records()
                            .forEachRemaining(subKr -> {
                                cachedKeys.add(subKr.key);
                                cache.insert(kr.key, kr.record);
                            });
                });

        return (Key[]) cachedKeys.stream().toArray();
    }

}
