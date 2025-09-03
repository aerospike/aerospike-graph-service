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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
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
            ID_MANAGER.recycleId(idsToRecycle.poll(), GRAPH);
            for (int i = 0; i < GRAPH.getBaseGraph().PHAT_EDGE_SIZE * 2; i++) {
                final FireflyPhatEdgeId id = getId(GRAPH);
                recycledPackIds.remove(id.getPackingId());
            }
        }
        Assert.assertTrue(recycledPackIds.isEmpty());
    }

    @Test
    public void testEdgeRecordIdBlocking() {
        Long currentEdgeRecordId = null;
        Set<Long> inUseRecordIdsSnapshot = null;
        int iterations = 0;
        while (iterations < 4) {
            final FireflyPhatEdgeId edgeId = getId(GRAPH);
            final Long storageId = (Long) edgeId.getStorageId();
            if (!storageId.equals(currentEdgeRecordId)) {
                if (currentEdgeRecordId != null) {
                    inUseRecordIdsSnapshot = ID_MANAGER.getInUseEdgeRecordIds();
                    Assert.assertFalse(inUseRecordIdsSnapshot.contains(currentEdgeRecordId));
                }
                currentEdgeRecordId = storageId;
                inUseRecordIdsSnapshot = ID_MANAGER.getInUseEdgeRecordIds();
                Assert.assertTrue(inUseRecordIdsSnapshot.contains(storageId));
                iterations++;
            } else {
                // When we're still using the same record ID, we check the last snapshot because pulling the last
                // packing ID for a record ID removes it from the set
                Assert.assertTrue(inUseRecordIdsSnapshot.contains(storageId));
                inUseRecordIdsSnapshot = ID_MANAGER.getInUseEdgeRecordIds();
            }
        }
    }

    @Test
    public void testGettingNewIdReleasesAndRecyclesRecordId() {
        FireflyPhatEdgeId edgeId = getId(GRAPH);
        Long storageId = (Long) edgeId.getStorageId();
        // Cycle until new pack
        while (true) {
            edgeId = getId(GRAPH);
            if (!storageId.equals((Long) edgeId.getStorageId())) {
                storageId = (Long) edgeId.getStorageId();
                break;
            }
        }
        Assert.assertTrue(ID_MANAGER.getInUseEdgeRecordIds().contains(storageId));
        Assert.assertFalse(ID_MANAGER.getRecycledPackIds().containsKey(storageId));
        final FireflyPhatEdgeId newId = FireflyPhatEdgeId.fromByteArray(ID_MANAGER.getNewId(GRAPH),
                GRAPH.getBaseGraph().PHAT_EDGE_SIZE, GRAPH.getBaseGraph().EDGE_AERO_SET);
        final Long newStorageId = (Long) newId.getStorageId();
        Assert.assertNotEquals(storageId, newStorageId);
        Assert.assertFalse(ID_MANAGER.getInUseEdgeRecordIds().contains(storageId));
        Assert.assertTrue(ID_MANAGER.getInUseEdgeRecordIds().contains(newStorageId));
        Assert.assertTrue(ID_MANAGER.getRecycledPackIds().containsKey(storageId));
        Assert.assertEquals(GRAPH.getBaseGraph().PHAT_EDGE_SIZE - 1, ID_MANAGER.getRecycledPackIds().get(storageId).size());
    }

    @Test
    public void testCanRecycleToInUseRecordId() throws Exception {
        final AtomicBoolean breakBoolean = new AtomicBoolean(false);
        final AtomicLong recycleStorageId = new AtomicLong(Long.MAX_VALUE);
        final AtomicBoolean internalAssertFailure = new AtomicBoolean(false);
        final Thread t = new Thread(() -> {
            FireflyPhatEdgeId id = getId(GRAPH);
            Long storageId = (Long) id.getStorageId();
            recycleStorageId.set(storageId);
            ID_MANAGER.recycleId(id, GRAPH);
            id = getId(GRAPH);
            storageId = (Long) id.getStorageId();
            if (!storageId.equals(recycleStorageId.get())) {
                internalAssertFailure.set(true);
            }
            ID_MANAGER.recycleId(id, GRAPH);
            while (!breakBoolean.get()) {
                try {
                    Thread.sleep(100);
                } catch (final InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });
        t.start();

        FireflyPhatEdgeId edgeId = getId(GRAPH);
        Long storageId = (Long) edgeId.getStorageId();
        // Cycle until new pack
        while (true) {
            edgeId = getId(GRAPH);
            if (!storageId.equals(edgeId.getStorageId())) {
                storageId = (Long) edgeId.getStorageId();
                break;
            }
        }
        while (recycleStorageId.get() == Long.MAX_VALUE) {
            Thread.sleep(10);
        }
        Assert.assertTrue(ID_MANAGER.getInUseEdgeRecordIds().contains(recycleStorageId.get()));
        Assert.assertTrue(ID_MANAGER.getRecycledPackIds().containsKey(recycleStorageId.get()));
        // Cycle until new pack and make sure we don't get the Record ID even though there's a key in the recycling map
        while (true) {
            edgeId = getId(GRAPH);
            Assert.assertNotEquals(edgeId.getStorageId(), recycleStorageId.get());
            if (!storageId.equals(edgeId.getStorageId())) {
                break;
            }
        }
        breakBoolean.set(true);
        t.join();
        Assert.assertFalse("Should not have mismatched Record IDs in thread", internalAssertFailure.get());
    }

    @Test
    public void testFinalizeReleasesRecordIdAndRecycles() throws Exception {
        final Set<Long> storageIdsUsed = ConcurrentHashMap.newKeySet();
        final List<Thread> threads = new ArrayList<>();
        final AtomicBoolean internalAssertFailure = new AtomicBoolean(false);
        for (int i = 0; i < 4; i++) {
            final Thread t = new Thread(() -> {
                final FireflyPhatEdgeId id = getId(GRAPH);
                final Long storageId = (Long) id.getStorageId();
                if (storageIdsUsed.contains(storageId)) {
                    internalAssertFailure.set(true);
                }
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            t.start();
            threads.add(t);
        }
        for (final Long storageId : storageIdsUsed) {
            Assert.assertTrue(ID_MANAGER.getInUseEdgeRecordIds().contains(storageId));
            Assert.assertFalse(ID_MANAGER.getRecycledPackIds().containsKey(storageId));
        }
        // Join all the threads, deference them, and wait for garbage collection
        for (final Thread t : threads) {
            t.join();
        }
        final long startTime = System.currentTimeMillis();
        final Set<Long> cleanedIds = new HashSet<>();
        while (System.currentTimeMillis() > startTime + 10000) {
            System.gc();
            boolean containsIdStillInUse = false;
            final Set<Long> inUseIds = ID_MANAGER.getInUseEdgeRecordIds();
            for (final Long storageId : storageIdsUsed) {
                if (inUseIds.contains(storageId)) {
                    containsIdStillInUse = true;
                } else {
                    cleanedIds.add(storageId);
                }
            }
            if (!containsIdStillInUse) {
                // All the threads were garbage collected
                break;
            }
        }
        if (System.currentTimeMillis() > startTime + 10000) {
            // If we timed out, just check to see if any threads were garbage collected as one thread working proves it works
            Assert.assertFalse(cleanedIds.isEmpty());
            // Find the cleaned IDs
            final Set<Long> inUseRecordIds = ID_MANAGER.getInUseEdgeRecordIds();
            final Map<Long, MrtRecyclingBufferedNumericIdManager.EdgePackIds> recyclingPacks = ID_MANAGER.getRecycledPackIds();
            for (final Long storageId : cleanedIds) {
                Assert.assertFalse(inUseRecordIds.contains(storageId));
                Assert.assertTrue(recyclingPacks.containsKey(storageId));
                Assert.assertEquals(GRAPH.getBaseGraph().PHAT_EDGE_SIZE - 1, recyclingPacks.get(storageId).size());
            }
        } else {
            final Set<Long> inUseRecordIds = ID_MANAGER.getInUseEdgeRecordIds();
            final Map<Long, MrtRecyclingBufferedNumericIdManager.EdgePackIds> recyclingPacks = ID_MANAGER.getRecycledPackIds();
            for (final Long storageId : storageIdsUsed) {
                Assert.assertFalse(inUseRecordIds.contains(storageId));
                Assert.assertTrue(recyclingPacks.containsKey(storageId));
                Assert.assertEquals(GRAPH.getBaseGraph().PHAT_EDGE_SIZE - 1, recyclingPacks.get(storageId).size());
            }
        }
    }

    private FireflyPhatEdgeId getId(final FireflyGraph graph) {
        return FireflyPhatEdgeId.fromByteArray(ID_MANAGER.getNextId(graph), graph.getBaseGraph().PHAT_EDGE_SIZE,
                graph.getBaseGraph().EDGE_AERO_SET);
    }
}
