package com.aerospike.firefly.process.call;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class QueryServiceAbortTest {
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph SETUP_GRAPH;
    private FireflyGraph graph;

    @BeforeClass
    static public void beforeAll() throws Exception {
        SETUP_GRAPH = FireflyGraph.open(CONFIG);
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, true);
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 32; i++) {
            final Thread thread = new Thread(() -> {
                final GraphTraversalSource g = SETUP_GRAPH.traversal();
                for (int j = 0; j < 100000; j++) {
                    g.addV().iterate();
                }
            });
            thread.start();
            threads.add(thread);
        }
        for (final Thread thread : threads) {
            thread.join();
        }
    }

    @AfterClass
    static public void afterAll() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, true);
        SETUP_GRAPH.close();
    }

    @Before
    public void beforeEach() {
        graph = FireflyGraph.open(CONFIG);
    }

    @After
    public void afterEach() {
        graph.close();
    }

    @Test
    public void testQueryServiceAbortAll() throws Exception {
        final GraphTraversalSource g = graph.traversal();
        final int scanCount = 4;
        final List<Thread> scanThreads = new ArrayList<>(scanCount);
        final List<AtomicBoolean> threadSuccesses = new ArrayList<>(scanCount);
        final CyclicBarrier barrier = new CyclicBarrier(scanCount + 1);
        for (int i = 0; i < scanCount; i++) {
            final int finalI = i;
            final AtomicBoolean assertion = new AtomicBoolean(false);
            final Thread scanThread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                try {
                    final var scan = g.V().has("propertyKey", String.valueOf(finalI));
                    while (scan.hasNext()) {
                        scan.next();
                    }
                    Assert.fail("Scan should have been aborted.");
                } catch (final Exception e) {
                    if (e.getCause() instanceof AerospikeException) {
                        final AerospikeException ae = (AerospikeException) e.getCause();
                        if (ae.getResultCode() == ResultCode.SCAN_ABORT) {
                            assertion.set(true);
                        }
                    } else {
                        throw e;
                    }
                }
            });
            scanThread.start();
            scanThreads.add(scanThread);
            threadSuccesses.add(assertion);
        }
        barrier.await();
        Thread.sleep(40);
        Map<String, Integer> queryAbortResult = (Map<String, Integer>) g.call("aerospike.graph.admin.query.abort").next();
        Assert.assertEquals(scanCount, (int) queryAbortResult.get("found"));
        Assert.assertEquals(scanCount, (int) queryAbortResult.get("aborted"));
        for (final Thread scanThread : scanThreads) {
            scanThread.join();
        }
        for (final AtomicBoolean assertion : threadSuccesses) {
            Assert.assertTrue(assertion.get());
        }
        queryAbortResult = (Map<String, Integer>) g.call("aerospike.graph.admin.query.abort").next();
        Assert.assertEquals(0, (int) queryAbortResult.get("found"));
        Assert.assertEquals(0, (int) queryAbortResult.get("aborted"));
    }
}
