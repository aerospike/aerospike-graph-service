package com.aerospike.firefly.tx;

import com.aerospike.firefly.util.exceptions.GraphError;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.aerospike.firefly.util.exceptions.GraphError.QUERY_IN_TRANSACTION;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;

public class TestTinkerpopTransactions {
    private static final String HOST = "172.17.0.1";
    private static final int PORT = 8182;
    private static DriverRemoteConnection REMOTE;
    private GraphTraversalSource g;

    @BeforeClass
    static public void beforeAll() {
        REMOTE = DriverRemoteConnection.using(HOST, PORT, "g");
    }

    @AfterClass
    static public void afterAll() throws Exception {
        if (REMOTE != null) {
            REMOTE.close();
        }
    }

    @Before
    public void beforeEach() {
        g = traversal().withRemote(REMOTE);
        g.V().drop().iterate();
    }

    @After
    public void afterEach() {
        g.V().drop().iterate();
    }

    @Test
    public void shouldCommit() throws InterruptedException {
        GraphTraversalSource gtx = this.g.tx().begin();
        Vertex v1 = gtx.addV().next();
        Vertex v2 = gtx.addV().next();
        Assert.assertEquals(2L, (long) gtx.V(v1.id(), v2.id()).count().next());
        this.countElementsInNewThreadTx(this.g, 0L, 0L);
        gtx.tx().commit();
        GraphTraversalSource gtx3 = this.g.tx().begin();
        Assert.assertEquals(2L, (long) gtx3.V(v1.id(), v2.id()).count().next());
        this.countElementsInNewThreadTx(this.g, 2L, 0L);
    }

    @Test
    public void shouldDeleteVertexOnCommit() throws InterruptedException {
        GraphTraversalSource gtx = this.g.tx().begin();
        Vertex v1 = gtx.addV().next();
        gtx.tx().commit();
        GraphTraversalSource gtx2 = this.g.tx().begin();
        Assert.assertEquals(1L, (long) gtx2.V(v1.id()).count().next());
        gtx2.V(v1.id()).drop().iterate();
        Assert.assertEquals(0L, (long) gtx2.V(v1.id()).count().next());
        this.countElementsInNewThreadTx(this.g, 1L, 0L);
        gtx2.tx().commit();
        GraphTraversalSource gtx3 = this.g.tx().begin();
        Assert.assertEquals(0L, (long) gtx3.V(v1.id()).count().next());
        this.countElementsInNewThreadTx(this.g, 0L, 0L);
    }

    @Test
    public void shouldDeleteRelatedEdgesOnVertexDelete() throws InterruptedException {
        GraphTraversalSource gtx = this.g.tx().begin();
        Vertex v1 = gtx.addV().next();
        Vertex v2 = gtx.addV().next();
        Edge e = gtx.addE("tests").from(v1).to(v2).next();
        gtx.tx().commit();
        GraphTraversalSource gtx2 = this.g.tx().begin();
        Assert.assertEquals(2L, (long) gtx2.V(v1.id(), v2.id()).count().next());
        Assert.assertEquals(1L, (long) gtx2.E(e.id()).count().next());
        gtx2.V(v1.id()).drop().iterate();
        gtx2.tx().commit();
        Assert.assertEquals(1L, (long) g.V().count().next());
        Assert.assertEquals(0L, (long) g.E().count().next());
        this.countElementsInNewThreadTx(this.g, 1L, 0L);
    }

    @Test
    public void shouldChangeVertexProperty() {
        GraphTraversalSource gtx = this.g.tx().begin();
        Vertex v1 = gtx.addV().property("test", 1).next();
        gtx.tx().commit();
        GraphTraversalSource gtx2 = this.g.tx().begin();
        Assert.assertEquals(1, gtx2.V(v1.id()).values("test").next());
        gtx2.V(v1.id()).property("test", 2).iterate();
        gtx2.tx().commit();
        Assert.assertEquals(1L, (long) g.V().count().next());
        Assert.assertEquals(2, g.V(v1.id()).values("test").next());
    }

    @Test
    public void shouldRollbackAddedVertex() throws InterruptedException {
        final GraphTraversalSource gtx = g.tx().begin();

        final Object vid1 = gtx.addV().id().next();
        final Object vid2 = gtx.addV().id().next();

        assertEquals(2, (long) gtx.V(vid1, vid2).count().next());

        // test count in second transaction before commit
        countElementsInNewThreadTx(g, 0, 0);

        gtx.tx().rollback();

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(0L, (long) gtx3.V(vid1, vid2).count().next());

        countElementsInNewThreadTx(g, 0, 0);
    }

    @Test
    public void shouldCommitEdge() throws InterruptedException {
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
        final Vertex v2 = gtx.addV().next();
        final Edge e = gtx.addE("tests").from(v1).to(v2).next();

        assertEquals(2, (long) gtx.V(v1.id(), v2.id()).count().next());
        assertEquals(1, (long) gtx.E(e.id()).count().next());

        countElementsInNewThreadTx(g, 0, 0);

        gtx.tx().commit();

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(2L, (long) gtx3.V(v1.id(), v2.id()).count().next());
        assertEquals(1L, (long) gtx3.E(e.id()).count().next());

        countElementsInNewThreadTx(g, 2, 1);
    }

    @Test
    public void shouldRollbackAddedEdge() throws InterruptedException {
        final GraphTraversalSource gtx = g.tx().begin();

        final Vertex v1 = gtx.addV().next();
        final Vertex v2 = gtx.addV().next();
        final Edge e = gtx.addE("tests").from(v1).to(v2).next();

        assertEquals(2, (long) gtx.V(v1.id(), v2.id()).count().next());
        assertEquals(1, (long) gtx.E(e.id()).count().next());

        // test count in second thread before commit
        final Thread thread = new Thread(() -> {
            try {
                countElementsInNewThreadTx(g, 0, 0);
            } catch (InterruptedException ex) {
                throw new RuntimeException(ex);
            }
        });
        thread.start();
        thread.join();

        gtx.tx().rollback();

        final GraphTraversalSource gtx3 = g.tx().begin();
        assertEquals(0L, (long) gtx3.V(v1.id(), v2.id()).count().next());
        assertEquals(0L, (long) gtx3.E(e.id()).count().next());

        countElementsInNewThreadTx(g, 0, 0);
    }

    @Test
    public void testTxCommitBlockedByOtherTx() {
        final Vertex v1 = g.addV().next();
        final Vertex v2 = g.addV().next();

        final GraphTraversalSource gtx = g.tx().begin();
        final GraphTraversalSource gtx2 = g.tx().begin();

        final Edge edge = gtx.addE("tests").from(v1).to(v2).next();
        final Object tx2id = gtx2.addV("tx2").id().next();
        try {
            gtx2.V(v1.id()).drop().iterate();
            Assert.fail("Accessing a blocked record from a different transaction should fail.");
        } catch (final Exception e) {
            Assert.assertTrue(e.getMessage().contains("Error code 120"));
        }
        Assert.assertEquals(0, (long) g.V(tx2id).count().next());
        // Should be able to commit operations that were unrelated to failure if desired.
        gtx2.tx().commit();
        Assert.assertEquals(1, (long) g.V(tx2id).count().next());
        Assert.assertEquals(0, (long) g.E().count().next());
        gtx.tx().commit();
        Assert.assertEquals(1, (long) g.E().count().next());
    }

    @Test
    public void testTraversalAfterRollback() {
        Assert.assertEquals(0, (long) g.V().count().next());
        final GraphTraversalSource gtx = g.tx().begin();
        gtx.addV().next();
        Assert.assertEquals(0, (long) g.V().count().next());
        gtx.tx().rollback();
        Assert.assertEquals(0, (long) g.V().count().next());
        try {
            gtx.addV().next();
            Assert.fail("Traversal after rollback should fail.");
        } catch (final Exception e) {
            Assert.assertTrue(e.getCause().getMessage().contains("Client is closed"));
        }
        Assert.assertEquals(0, (long) g.V().count().next());
    }

    @Test
    public void testCommitAfterRollback() {
        Assert.assertEquals(0, (long) g.V().count().next());
        final GraphTraversalSource gtx = g.tx().begin();
        gtx.addV().next();
        Assert.assertEquals(0, (long) g.V().count().next());
        gtx.tx().rollback();
        Assert.assertEquals(0, (long) g.V().count().next());
        try {
            gtx.tx().commit();
            Assert.fail("Commit after rollback should fail.");
        } catch (final Exception e) {
            Assert.assertTrue(e.getCause().getMessage().contains("Client is closed"));
        }
        Assert.assertEquals(0, (long) g.V().count().next());
    }

    @Test
    public void testRollbackAfterRollback() {
        Assert.assertEquals(0, (long) g.V().count().next());
        final GraphTraversalSource gtx = g.tx().begin();
        gtx.addV().next();
        Assert.assertEquals(0, (long) g.V().count().next());
        gtx.tx().rollback();
        Assert.assertEquals(0, (long) g.V().count().next());
        try {
            gtx.tx().rollback();
            Assert.fail("Rollback after rollback should fail.");
        } catch (final Exception e) {
            Assert.assertTrue(e.getCause().getMessage().contains("Client is closed"));
        }
        Assert.assertEquals(0, (long) g.V().count().next());
    }

    @Test
    public void testTraversalAfterCommit() {
        Assert.assertEquals(0, (long) g.V().count().next());
        final GraphTraversalSource gtx = g.tx().begin();
        gtx.addV().next();
        Assert.assertEquals(0, (long) g.V().count().next());
        gtx.tx().commit();
        Assert.assertEquals(1, (long) g.V().count().next());
        try {
            gtx.addV().next();
            Assert.fail("Traversal after commit should fail.");
        } catch (final Exception e) {
            Assert.assertTrue(e.getCause().getMessage().contains("Client is closed"));
        }
        Assert.assertEquals(1, (long) g.V().count().next());
    }

    @Test
    public void testCommitAfterCommit() {
        Assert.assertEquals(0, (long) g.V().count().next());
        final GraphTraversalSource gtx = g.tx().begin();
        gtx.addV().next();
        Assert.assertEquals(0, (long) g.V().count().next());
        gtx.tx().commit();
        Assert.assertEquals(1, (long) g.V().count().next());
        try {
            gtx.tx().commit();
            Assert.fail("Commit after commit should fail.");
        } catch (final Exception e) {
            Assert.assertTrue(e.getCause().getMessage().contains("Client is closed"));
        }
        Assert.assertEquals(1, (long) g.V().count().next());
    }

    @Test
    public void testRollbackAfterCommit() {
        Assert.assertEquals(0, (long) g.V().count().next());
        final GraphTraversalSource gtx = g.tx().begin();
        gtx.addV().next();
        Assert.assertEquals(0, (long) g.V().count().next());
        gtx.tx().commit();
        Assert.assertEquals(1, (long) g.V().count().next());
        try {
            gtx.tx().rollback();
            Assert.fail("Rollback after commit should fail.");
        } catch (final Exception e) {
            Assert.assertTrue(e.getCause().getMessage().contains("Client is closed"));
        }
        Assert.assertEquals(1, (long) g.V().count().next());
    }

    @Test
    public void testScanInTx() {
        g.addV("test").next();
        final GraphTraversalSource gtx = g.tx().begin();
        try {
            gtx.V().hasLabel("test").next();
            Assert.fail("Scan should not work in tx.");
        } catch (final Exception e) {
            Assert.assertTrue(e.getMessage().contains(GraphError.getMessage(QUERY_IN_TRANSACTION)));
        }
    }

    @Test
    public void testQueryInTx() {
        g.addV("test").next();
        final GraphTraversalSource gtx = g.tx().begin();
        try {
            gtx.V().hasLabel("test").next();
            Assert.fail("Query should not work in tx.");
        } catch (final Exception e) {
            Assert.assertTrue(e.getMessage().contains(GraphError.getMessage(QUERY_IN_TRANSACTION)));
        }
    }

    @Test
    public void testTxUsesMrtEdgeIdManager() {
        final Vertex v1 = g.addV().next();
        final Vertex v2 = g.addV().next();
        final Vertex v3 = g.addV().next();
        final Vertex v4 = g.addV().next();
        Assert.assertEquals(0, (long) g.E().count().next());

        final LinkedList<String> ids = new LinkedList<>();
        final AtomicInteger dropCounter = new AtomicInteger(0);
        final Random rng = new Random();
        final Vertex fromVDrop = g.addV().next();
        final Vertex toVDrop = g.addV().next();
        for (int i = 0; i < 400; i++) {
            // These are tightly packed edge records that share record IDs we're going to recycle IDs of.
            final String id = (String) g.addE("drop").from(fromVDrop).to(toVDrop).id().next();
            ids.add(id);
        }
        Assert.assertEquals(400, (long) g.E().count().next());

        final GraphTraversalSource gtx1 = g.tx().begin();
        final GraphTraversalSource gtx2 = g.tx().begin();

        final Vertex v5 = gtx1.addV().next();
        final Vertex v6 = gtx1.addV().next();
        final Vertex v7 = gtx2.addV().next();
        final Vertex v8 = gtx2.addV().next();

        for (int i = 0; i < 100; i++) {
            gtx1.addE("tx1").from(v1).to(v2).iterate();
            dropEAndCatchTransactionConflict(g, ids, dropCounter, rng);
            gtx2.addE("tx2").from(v3).to(v4).iterate();
            dropEAndCatchTransactionConflict(g, ids, dropCounter, rng);
            gtx1.addE("tx1").from(v5).to(v6).iterate();
            dropEAndCatchTransactionConflict(g, ids, dropCounter, rng);
            gtx2.addE("tx2").from(v7).to(v8).iterate();
            dropEAndCatchTransactionConflict(g, ids, dropCounter, rng);
        }

        gtx1.tx().commit();
        gtx2.tx().commit();

        Assert.assertEquals(200, (long) g.E().hasLabel("tx1").count().next());
        Assert.assertEquals(200, (long) g.E().hasLabel("tx2").count().next());
        Assert.assertTrue(dropCounter.get() > 0);
        Assert.assertTrue(dropCounter.get() <= 400);
        Assert.assertEquals(0, ids.size());
        Assert.assertEquals((400 - dropCounter.get()), (long) g.E().hasLabel("drop").count().next());
    }

    @Test
    public void testExceedRecordCount() {
        GraphTraversalSource gtx = this.g.tx().begin();
        try {
            for (int i = 1; i < 5000; i++) {
                gtx.addV().next();
            }
            gtx.tx().commit();
            Assert.fail("Should have throw an exception for exceeding the MRT record limit.");
        } catch (final Exception e) {
            gtx.tx().rollback();
            Assert.assertTrue(e.getMessage().contains(GraphError.getMessage(GraphError.TX_RECORD_LIMIT_EXCEEDED)));
        }
    }
    private void dropEAndCatchTransactionConflict(final GraphTraversalSource g, final List<String> ids,
                                                  final AtomicInteger dropCounter, final Random rng) {
        final int indexToRemove = rng.nextInt(ids.size());
        final String id = ids.get(indexToRemove);
        Assert.assertTrue(ids.remove(id));
        try {
            g.E(id).drop().iterate();
            dropCounter.incrementAndGet();
        } catch (final Exception e) {
            final boolean containsMessage = e.getMessage().contains("Error code 120");
            if (!containsMessage) {
                Assert.fail(e.getMessage());
            }
        }
    }

    private void countElementsInNewThreadTx(final GraphTraversalSource g, final long verticesCount,
                                            final long edgesCount) throws InterruptedException {
        AtomicLong vCount = new AtomicLong(-1L);
        AtomicLong eCount = new AtomicLong(-1L);
        Thread thread = new Thread(() -> {
            vCount.set(g.V(new Object[0]).count().next());
            eCount.set(g.E(new Object[0]).count().next());
        });
        thread.start();
        thread.join();
        Assert.assertEquals(verticesCount, vCount.get());
        Assert.assertEquals(edgesCount, eCount.get());
    }
}
