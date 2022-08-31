package com.aerospike.firefly.io.txn;

import com.aerospike.firefly.util.PerfUtil;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.janusgraph.core.JanusGraphFactory;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TransactionTests {
    private Graph graph;

    @Before
    public void setup() {
        //@todo: replace with FireflyGraph when transaction support is introduced
        //shim Janus for transaction tests, included as test dependency
        graph = JanusGraphFactory.build().set("storage.backend", "inmemory").open();
    }

    @Test
    public void testRollbackBasicTxn() {
        long startTime = System.currentTimeMillis();
        Transaction tx = graph.tx();
        int groups = 100;
        tx.open();
        GraphTraversalSource gtx = tx.begin();
        for (int j = 0; j < 100 * groups; j++) {
            gtx.addV();
        }
        long writeTime = System.currentTimeMillis();
        // capture time to write to tx.
        tx.rollback();
        long rollbackTime = System.currentTimeMillis();
        // capture time to rollback tx.
        System.out.printf("write time: %d  rollbackTime: %d %n", writeTime-startTime, rollbackTime-writeTime);
    }

    @Test
    public void testCommitTxn() {
        long startTime = System.currentTimeMillis();
        Transaction tx = graph.tx();
        int groups = 1000;
        tx.open();
        GraphTraversalSource gtx = tx.begin();
        for (int j = 0; j < 100 * groups; j++) {
            gtx.addV();
        }
        long writeTime = System.currentTimeMillis();
        // capture time to write to tx.
        tx.commit();
        long commitTime = System.currentTimeMillis();
        // capture time to rollback tx.
        System.out.printf("write time: %d  commit: %d %n", writeTime-startTime, commitTime-writeTime);
    }
}

