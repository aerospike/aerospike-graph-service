package com.aerospike.firefly.process.call;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
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
                for (int j = 0; j < 200000; j++) {
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
        boolean passedGlobal = false;
        for (int runCount = 0; runCount < 15; runCount++) {
            boolean passedLocal = true;
            final int scanCount = 8;
            final List<Thread> scanThreads = new ArrayList<>(scanCount);
            final CyclicBarrier barrier = new CyclicBarrier(scanCount + 1);
            for (int i = 0; i < scanCount; i++) {
                final int finalI = i;
                final Thread scanThread = new Thread(() -> {
                    try {
                        barrier.await();
                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    }
                    try {
                        System.out.println("Scan " + finalI + " started.");
                        final var scan = g.V().has("propertyKey", String.valueOf(finalI));
                        while (scan.hasNext()) {
                            scan.next();
                        }
                        System.out.println("Scan " + finalI + " finished.");
                    } catch (final Exception e) {
                        if (e.getCause() instanceof AerospikeException) {
                            final AerospikeException ae = (AerospikeException) e.getCause();
                            if (ae.getResultCode() != ResultCode.SCAN_ABORT) {
                                Assert.fail("Unexpected AerospikeException: " + ae);
                            }
                        } else {
                            Assert.fail("Unexpected Exception: " + e);
                        }
                    }
                });
                scanThread.start();
                scanThreads.add(scanThread);
            }
            barrier.await();
            Thread.sleep(300);
            System.out.println("Aborting all scans.");
            Map<String, Integer> queryAbortResult = (Map<String, Integer>) g.call("aerospike.graph.admin.query.abort").next();
            int foundQueries = queryAbortResult.get("found");
            int abortedQueries = queryAbortResult.get("aborted");
            passedLocal &= foundQueries > 0 && foundQueries <= scanCount && abortedQueries > 0 && abortedQueries <= foundQueries;
            for (final Thread scanThread : scanThreads) {
                scanThread.join();
            }
            queryAbortResult = (Map<String, Integer>) g.call("aerospike.graph.admin.query.abort").next();
            passedLocal &= 0 == queryAbortResult.get("found") && 0 == queryAbortResult.get("aborted");
            if (passedLocal) {
                passedGlobal = true;
                break;
            }
        }
        if (!passedGlobal) {
            Assert.fail("Test failed all iterations.");
        }
    }
}
