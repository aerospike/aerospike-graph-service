package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestMergeVConcurrent {

    static FireflyGraph SETUP_GRAPH;


    @BeforeClass
    static public void beforeClass() {
        SETUP_GRAPH = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES));
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, true);
    }

    @AfterClass
    static public void afterClass() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, true);
        SETUP_GRAPH.close();
    }

    @Test
    public void testMergeVId() throws InterruptedException {
        final GraphTraversalSource g = SETUP_GRAPH.traversal();
        int threadCount = 16;
        for (int j = 0; j < 100; j++) {
            final ExecutorService executorService = Executors.newFixedThreadPool(16);
            CountDownLatch latch = new CountDownLatch(threadCount);
            final AtomicBoolean failed = new AtomicBoolean(false);
            for (int i = 0; i < threadCount; i++) {
                final int finalI = i;
                final int finalJ = j;
                executorService.submit(() -> {
                    try {
                        latch.countDown();
                        try {
                            latch.await();
                        } catch (InterruptedException e) {
                            failed.set(true);
                        }
                        final Map<Object, Object> propertiesCreate = new HashMap<>();
                        final Map<Object, Object> propertiesMatch = new HashMap<>();
                        propertiesCreate.put(T.id, finalJ);
                        propertiesCreate.put("name" + finalI, finalI);
                        propertiesMatch.put("name" + finalI, finalI);
                        g.mergeV(CollectionUtil.asMap(T.id, finalJ))
                                .option(Merge.onMatch, propertiesMatch)
                                .option(Merge.onCreate, propertiesCreate).iterate();
                    } catch (Exception e) {
                        failed.set(true);
                    }
                });
            }
            executorService.shutdown();
            executorService.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
            final Vertex v = g.V(j).next();
            for (int i = 0; i < threadCount; i++) {
                Assert.assertEquals(i, v.property("name" + i).value());
            }
            Assert.assertFalse(failed.get());
        }
    }
}
