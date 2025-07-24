package com.aerospike.firefly.tx;

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

import java.util.concurrent.atomic.AtomicLong;

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
        gtx2.V(new Object[]{v1.id()}).drop().iterate();
        Assert.assertEquals(0L, (long) gtx2.V(v1.id()).count().next());
        this.countElementsInNewThreadTx(this.g, 1L, 0L);
        gtx2.tx().commit();
        GraphTraversalSource gtx3 = this.g.tx().begin();
        Assert.assertEquals(0L, (long) gtx3.V(v1.id()).count().next());
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
        gtx2.V(new Object[]{v1.id()}).drop().iterate();
        gtx2.tx().commit();
        Assert.assertEquals(1L, (long) g.V(new Object[0]).count().next());
        Assert.assertEquals(0L, (long) g.E(new Object[0]).count().next());
        this.countElementsInNewThreadTx(this.g, 1L, 0L);
    }

    @Test
    public void shouldChangeVertexProperty() {
        GraphTraversalSource gtx = this.g.tx().begin();
        Vertex v1 = gtx.addV().property("test", 1, new Object[0]).next();
        gtx.tx().commit();
        GraphTraversalSource gtx2 = this.g.tx().begin();
        Assert.assertEquals(1, gtx2.V(new Object[]{v1.id()}).values("test").next());
        gtx2.V(new Object[]{v1.id()}).property("test", 2, new Object[0]).iterate();
        gtx2.tx().commit();
        Assert.assertEquals(1L, (long) g.V(new Object[0]).count().next());
        Assert.assertEquals(2, g.V(new Object[]{v1.id()}).values("test").next());
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
