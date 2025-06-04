package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class MrtRecyclingBufferedNumericIdManagerTest {
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph GRAPH;
    private static MrtRecyclingBufferedNumericIdManager ID_MANAGER;

    @BeforeClass
    static public void beforeAll() throws NoSuchFieldException, IllegalAccessException {
        GRAPH = FireflyGraph.open(CONFIG);
        final AerospikeConnection db = GRAPH.getBaseGraph();
        final Field mrtEnabled = db.getClass().getDeclaredField("MRT_ENABLED");
        mrtEnabled.setAccessible(true);
        mrtEnabled.set(db, true);
        ID_MANAGER = (MrtRecyclingBufferedNumericIdManager) new FireflyIdFactory(db).getEdgeIdManager();
    }

    @AfterClass
    static public void afterAll() {
        GRAPH.close();
    }

    @Test
    public void testThreadsGrabDiscreteEdgePackIds() throws InterruptedException {
        final Set<Long> usedStorageIds = ConcurrentHashMap.newKeySet();
        final List<Thread> threads = new ArrayList<>();
        final CyclicBarrier barrier = new CyclicBarrier(4);
        final AtomicReference<String> failure = new AtomicReference<>();
        for (int i = 0; i < 4; i++) {
            final Thread t = new Thread(() -> {
                try {
                    barrier.await();
                } catch (final Exception e) {
                    Assert.fail(e.getMessage());
                }
                final long startTime = System.currentTimeMillis();
                long lastStorageId = Long.MAX_VALUE;
                int lastIdSeenCount = 0;
                while (System.currentTimeMillis() - startTime < 1000) {
                    final FireflyPhatEdgeId id = getId(GRAPH);
                    final long storageId = (long) id.getStorageId();
                    if (storageId != lastStorageId) {
                        // This is a new set of IDs
                        if (usedStorageIds.contains(storageId)) {
                            failure.set("An Edge pack's ID was shared to a different thread.");
                            return;
                        }
                        usedStorageIds.add(storageId);
                        lastIdSeenCount = 1;
                    } else {
                        lastIdSeenCount++;
                        if (lastIdSeenCount > GRAPH.getBaseGraph().PHAT_EDGE_SIZE) {
                            failure.set("The ID manager generated the same storage ID more than the phat edge size.");
                            return;
                        }
                    }
                    lastStorageId = storageId;
                }
            });
            threads.add(t);
        }
        for (final Thread t : threads) {
            t.start();
        }
        for (final Thread t : threads) {
            t.join();
        }
        if (failure.get() != null) {
            Assert.fail(failure.get());
        }
    }

    @Test
    public void testIdRecycling() {
        final Set<Long> recycledPackIds = ConcurrentHashMap.newKeySet();
        final Queue<FireflyId> idsToRecycle = new ArrayDeque<>();
        for (int i = 0; i < GRAPH.getBaseGraph().PHAT_EDGE_SIZE; i++) {
            final FireflyPhatEdgeId id = getId(GRAPH);
            idsToRecycle.add(id);
            recycledPackIds.add(id.getPackingId());
        }

        while (!idsToRecycle.isEmpty()) {
            ID_MANAGER.recycleId(idsToRecycle.poll());
            for (int i = 0; i < GRAPH.getBaseGraph().PHAT_EDGE_SIZE * 2; i++) {
                final FireflyPhatEdgeId id = getId(GRAPH);
                recycledPackIds.remove(id.getPackingId());
            }
        }
        Assert.assertTrue(recycledPackIds.isEmpty());
    }

    private FireflyPhatEdgeId getId(final FireflyGraph graph) {
        return FireflyPhatEdgeId.fromByteArray(ID_MANAGER.getNextId(graph), graph.getBaseGraph().PHAT_EDGE_SIZE,
                graph.getBaseGraph().EDGE_AERO_SET);
    }
}
