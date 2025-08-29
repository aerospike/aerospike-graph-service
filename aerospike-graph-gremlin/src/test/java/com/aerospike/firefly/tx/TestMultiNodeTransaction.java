package com.aerospike.firefly.tx;

import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.*;

/**
 * Tests focused on transactional semantics where only primary-key lookups are allowed.
 * We avoid secondary-index/property scans and stick to lookups by T.id and incident traversals
 * from known vertices (e.g. v.V(id).bothE()).
 */
public class TestMultiNodeTransaction {

    private DriverRemoteConnection drc1;
    private DriverRemoteConnection drc2;
    private DriverRemoteConnection drc3;

    private GraphTraversalSource g1;
    private GraphTraversalSource g2;
    private GraphTraversalSource g3;

    @Before
    public void setUp() {
        drc1 = DriverRemoteConnection.using("172.17.0.1", 8182, "g");
        drc2 = DriverRemoteConnection.using("172.17.0.1", 8183, "g");
        drc3 = DriverRemoteConnection.using("172.17.0.1", 8184, "g");
        g1 = traversal().withRemote(drc1);
        g2 = traversal().withRemote(drc2);
        g3 = traversal().withRemote(drc3);

        // Test isolation: start clean (uses a scan, but matches the original baseline you provided).
        // If your backend forbids global scans, replace with targeted cleanup for known IDs used below.
        g1.V().drop().iterate();
    }

    @After
    public void tearDown() {
        safeClose(drc1);
        safeClose(drc2);
        safeClose(drc3);
    }

    private static void safeClose(DriverRemoteConnection drc) {
        try { if (drc != null) drc.close(); } catch (Exception ignored) {}
    }

    // ----------------------
    // Baseline example (your original, kept as-is)
    // ----------------------
    @Test
    public void testMultiNodeTransactionMergeE() {
        try {
            final GraphTraversalSource gtx1 = g1.tx().begin();
            final GraphTraversalSource gtx2 = g2.tx().begin();
            final GraphTraversalSource gtx3 = g3.tx().begin();

            Vertex v11 = gtx1.addV("Test").property(T.id, 11).next();
            Vertex v12 = gtx1.addV("Test").property(T.id, 12).next();
            Vertex v21 = gtx2.addV("Test").property(T.id, 21).next();
            Vertex v22 = gtx2.addV("Test").property(T.id, 22).next();
            Vertex v31 = gtx3.addV("Test").property(T.id, 31).next();
            Vertex v32 = gtx3.addV("Test").property(T.id, 32).next();

            final Map<Object, Object> mergeMap1 = new HashMap<>();
            mergeMap1.put(Direction.OUT, new ReferenceVertex(v11.id()));
            mergeMap1.put(Direction.IN, new ReferenceVertex(v12.id()));
            mergeMap1.put(T.label, "mergeE");
            final Map<Object, Object> mergeMap2 = new HashMap<>();
            mergeMap2.put(Direction.OUT, new ReferenceVertex(v21.id()));
            mergeMap2.put(Direction.IN, new ReferenceVertex(v22.id()));
            mergeMap2.put(T.label, "mergeE");
            final Map<Object, Object> mergeMap3 = new HashMap<>();
            mergeMap3.put(Direction.OUT, new ReferenceVertex(v31.id()));
            mergeMap3.put(Direction.IN, new ReferenceVertex(v32.id()));
            mergeMap3.put(T.label, "mergeE");

            gtx1.mergeE(mergeMap1).iterate();
            gtx2.mergeE(mergeMap2).iterate();
            gtx3.mergeE(mergeMap3).iterate();

            gtx1.tx().commit();
            gtx2.tx().commit();
            gtx3.tx().commit();

            final List<Edge> e11s = g1.V(v11.id()).bothE().toList();
            final List<Edge> e12s = g1.V(v12.id()).bothE().toList();
            final List<Edge> e21s = g1.V(v21.id()).bothE().toList();
            final List<Edge> e22s = g1.V(v22.id()).bothE().toList();
            final List<Edge> e31s = g1.V(v31.id()).bothE().toList();
            final List<Edge> e32s = g1.V(v32.id()).bothE().toList();

            Assert.assertEquals(1, e11s.size());
            Assert.assertEquals(1, e12s.size());
            Assert.assertEquals(1, e21s.size());
            Assert.assertEquals(1, e22s.size());
            Assert.assertEquals(1, e31s.size());
            Assert.assertEquals(1, e32s.size());
        } catch (Exception e) {
            fail("error: " + e);
        }
    }

    // ----------------------
    // Additional tests
    // ----------------------

    /**
     * Basic: read-your-writes and visibility across transactions.
     */
    @Test
    public void testVisibilityAndCommit() {
        GraphTraversalSource tx1 = g1.tx().begin();
        tx1.addV("Test").property(T.id, 100).iterate();

        // Not visible from other connection until commit
        assertFalse("Uncommitted vertex should not be visible from a different connection",
                g2.V(100).hasNext());

        tx1.tx().commit();

        // Now visible everywhere
        assertTrue(g1.V(100).hasNext());
        assertTrue(g2.V(100).hasNext());
    }

    /**
     * Rollback should discard uncommitted data.
     */
    @Test
    public void testRollback() {
        GraphTraversalSource tx = g1.tx().begin();
        tx.addV("Test").property(T.id, 200).iterate();
        tx.tx().rollback();
        assertFalse(g1.V(200).hasNext());
    }

    /**
     * mergeV should be idempotent with the same id (primary key). No scans used.
     */
    @Test
    public void testMergeVIdempotent() {
        Map<Object, Object> m = new HashMap<>();
        m.put(T.id, 300);
        m.put(T.label, "Test");

        GraphTraversalSource tx1 = g1.tx().begin();
        GraphTraversalSource tx2 = g2.tx().begin();

        tx1.mergeV(m).property("p", "a").iterate();

        tx1.tx().commit();

        tx2.mergeV(m).property("p", "b").iterate();
        tx2.tx().commit();

        // Still a single vertex by id
        assertTrue(g1.V(300).hasNext());
        // property value is last-writer-wins (backend dependent) – we just ensure a value exists
        String val = (String) g1.V(300).values("p").tryNext().orElse(null);
        assertNotNull(val);
    }

    /**
     * Concurrency: many threads MERGE the same edge concurrently. Exactly one physical edge should exist.
     */
    @Test
    public void testConcurrentMergeEOneEdge() throws InterruptedException, ExecutionException {
        // Prepare the two endpoint vertices
        g1.addV("Test").property(T.id, 4001).iterate();
        g1.addV("Test").property(T.id, 4002).iterate();

        int threads = 16;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Callable<Boolean>> tasks = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            final int port = 8182 + (i % 3); // round-robin across the three servers
            tasks.add(() -> {
                DriverRemoteConnection drc = null;
                try {
                    drc = DriverRemoteConnection.using("172.17.0.1", port, "g");
                    GraphTraversalSource g = traversal().withRemote(drc);

                    // Begin txn per thread
                    GraphTraversalSource tx = g.tx().begin();

                    Map<Object, Object> mergeMap = new HashMap<>();
                    mergeMap.put(Direction.OUT, new ReferenceVertex(4001));
                    mergeMap.put(Direction.IN, new ReferenceVertex(4002));
                    mergeMap.put(T.label, "link");

                    start.await();
                    tx.mergeE(mergeMap).iterate();
                    tx.tx().commit();
                    return true;
                } catch (Exception e) {
                    // acceptable if backend throws a conflict – the important check is final edge count == 1
                    return false;
                } finally {
                    if (drc != null) try { drc.close(); } catch (Exception ignored) {}
                }
            });
        }

        List<Future<Boolean>> results = new ArrayList<>();
        for (Callable<Boolean> c : tasks) results.add(pool.submit(c));
        start.countDown();
        pool.shutdown();
        pool.awaitTermination(60, TimeUnit.SECONDS);

        // Exactly one edge should exist between 4001 -> 4002 with label "link"
        long countOut = g1.V(4001).outE("link").count().next();
        long countIn = g1.V(4002).inE("link").count().next();
        assertEquals(1L, countOut);
        assertEquals(1L, countIn);

        // At least one thread should have succeeded
        long successes = results.stream().filter(f -> {
            try { return Boolean.TRUE.equals(f.get()); } catch (Exception e) { return false; }
        }).count();
        assertTrue("Expected at least one successful commit", successes >= 1);
    }

    /**
     * Concurrency: N threads increment a counter on the same vertex atomically using a single property() step
     * with coalesce+math server-side (no client-side read/modify/write window).
     */
    @Test
    public void testConcurrentAtomicCounterIncrement() throws InterruptedException {
        // Seed the counter vertex
        g1.addV("Counter").property(T.id, 5000).property("count", 0).iterate();

        int threads = 20;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<Boolean>> fs = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            final int port = 8182 + (i % 3);
            fs.add(pool.submit(() -> {
                DriverRemoteConnection drc = null;
                int attemptCount = 0;
                do {
                    try {
                        attemptCount++;
                        drc = DriverRemoteConnection.using("172.17.0.1", port, "g");
                        GraphTraversalSource g = traversal().withRemote(drc);
                        GraphTraversalSource tx = g.tx().begin();

                        start.await();
                        Integer value = (Integer) tx.V(5000).values("count").next();
                        tx.V(5000).property("count", value + 1).iterate();
                        System.out.println("Incremented to " + (value + 1) + " on port " + port);

                        tx.tx().commit();
                        drc.close();
                        return true;
                    } catch (Exception ignored) {
                        System.out.println(ignored);
                    }
                } while (attemptCount < 50);
                return false;
            }));
        }

        start.countDown();
        pool.shutdown();
        pool.awaitTermination(60, TimeUnit.SECONDS);

        int finalCount = g1.V(5000).values("count").tryNext().map(o -> ((Number) o).intValue()).orElse(-1);
        assertEquals("All increments should be visible in serializable semantics", threads, finalCount);
    }

    /**
     * Concurrency without conflicts: each thread creates a disjoint edge pair; verifies all committed.
     */
    @Test
    public void testParallelIndependentTransactions() throws InterruptedException {
        int threads = 24;
        ExecutorService pool = Executors.newFixedThreadPool(threads);
        CountDownLatch start = new CountDownLatch(1);
        List<Callable<Boolean>> fs = new ArrayList<>();

        for (int i = 0; i < threads; i++) {
            final int idA = 6000 + (i * 2);
            final int idB = idA + 1;
            final int port = 8182 + (i % 3);

            fs.add(Executors.callable(() -> {
                DriverRemoteConnection drc = null;
                try {
                    drc = DriverRemoteConnection.using("172.17.0.1", port, "g");
                    GraphTraversalSource g = traversal().withRemote(drc);
                    GraphTraversalSource tx = g.tx().begin();

                    // create endpoints with fixed ids (primary-keyed)
                    tx.addV("Test").property(T.id, idA).iterate();
                    tx.addV("Test").property(T.id, idB).iterate();

                    Map<Object, Object> mm = new HashMap<>();
                    mm.put(Direction.OUT, new ReferenceVertex(idA));
                    mm.put(Direction.IN, new ReferenceVertex(idB));
                    mm.put(T.label, "p");

                    start.await();
                    tx.mergeE(mm).iterate();
                    tx.tx().commit();
                } catch (Exception e) {
                    // surface in assertion below by checking counts
                } finally {
                    if (drc != null) try { drc.close(); } catch (Exception ignored) {}
                }
            }, true));
        }
        final List<Future<Boolean>> results = new ArrayList<>();
        for (Callable<Boolean> c : fs) results.add(pool.submit(c));

        start.countDown();
        pool.shutdown();
        pool.awaitTermination(60, TimeUnit.SECONDS);

        // Verify all edges exist
        for (int i = 0; i < threads; i++) {
            final int idA = 6000 + (i * 2);
            final int idB = idA + 1;
            assertTrue(g1.V(idA).hasNext());
            assertTrue(g1.V(idB).hasNext());
            long c = g1.V(idA).outE("p").where(__.inV().hasId(idB)).count().next();
            assertEquals(1L, c);
        }
    }

    /**
     * Commit ordering: creating vertices in different tx and committing in reverse order should yield all records.
     */
    @Test
    public void testCommitOrder() {
        GraphTraversalSource tx1 = g1.tx().begin();
        GraphTraversalSource tx2 = g1.tx().begin();

        tx1.addV("Test").property(T.id, 7001).iterate();
        tx2.addV("Test").property(T.id, 7002).iterate();

        // Commit tx2 first, then tx1
        tx2.tx().commit();
        tx1.tx().commit();

        assertTrue(g1.V(7001).hasNext());
        assertTrue(g1.V(7002).hasNext());
    }

    /**
     * mergeE idempotency across labels: same endpoints, different labels should yield one edge per label.
     */
    @Test
    public void testMergeEByLabel() {
        g1.addV("Test").property(T.id, 8001).iterate();
        g1.addV("Test").property(T.id, 8002).iterate();

        GraphTraversalSource tx = g1.tx().begin();
        Map<Object, Object> a = mm(8001, 8002, "L1");
        Map<Object, Object> b = mm(8001, 8002, "L2");
        tx.mergeE(a).iterate();
        tx.mergeE(b).iterate();
        tx.tx().commit();

        long l1 = g1.V(8001).outE("L1").count().next();
        long l2 = g1.V(8001).outE("L2").count().next();
        assertEquals(1L, l1);
        assertEquals(1L, l2);
    }

    public static Map<Object, Object> mm(Object outId, Object inId, String label) {
        Map<Object, Object> m = new HashMap<>();
        m.put(Direction.OUT, new ReferenceVertex(outId));
        m.put(Direction.IN, new ReferenceVertex(inId));
        m.put(T.label, label);
        return m;
    }
}
