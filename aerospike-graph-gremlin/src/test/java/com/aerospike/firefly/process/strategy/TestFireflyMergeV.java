/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestFireflyMergeV {

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

    @Before
    public void beforeEach() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
    }

    @Test
    public void testConcurrentMergeVOnId() throws InterruptedException {
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

    @Test
    public void testMismatchedSearchWithExistingId() {
        final GraphTraversalSource g = SETUP_GRAPH.traversal();
        g.addV("foo").property(T.id, 777).property("bar", 123).next();
        Assert.assertTrue(g.V(777).hasNext());
        try {
            g.mergeV(Map.of(T.id, 777, T.label, "baz")).iterate();
            Assert.fail("Should fail with inability to create existing id");
        } catch (final IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Vertex with id already exists:"));
        }
        try {
            g.mergeV(Map.of(T.id, 777, "bar", 456)).iterate();
            Assert.fail("Should fail with inability to create existing id");
        } catch (final IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Vertex with id already exists:"));
        }
    }

    @Test
    public void testOnMatchMultiProperties() {
        final GraphTraversalSource g = SETUP_GRAPH.traversal();
        g.addV("foo")
                .property(T.id, 777)
                .property("one", 1)
                .property("two", 2)
                .property("three", 3)
                .property("four", 4)
                .property("five", "five").next();
        Assert.assertTrue(g.V(777).hasNext());
        g.mergeV(Map.of(T.id, 777))
                .option(Merge.onMatch, Map.of(
                        "one", VertexProperty.Cardinality.list("one"),
                        "two", "two",
                        "three", VertexProperty.Cardinality.single("three"),
                        "four", VertexProperty.Cardinality.set("four"),
                        "five", VertexProperty.Cardinality.set("five"),
                        "six", VertexProperty.Cardinality.set("six")
                )).iterate();
        Assert.assertEquals(8, (long) g.V(777).properties().count().next());
        final Set<Object> expectedOne = new HashSet<>();
        expectedOne.add("one");
        expectedOne.add(1);
        g.V(777).properties("one").value().forEachRemaining(expectedOne::remove);
        Assert.assertTrue(expectedOne.isEmpty());
        Assert.assertEquals("two", g.V(777).properties("two").value().next());
        Assert.assertEquals("three", g.V(777).properties("three").value().next());
        final Set<Object> expectedFour = new HashSet<>();
        expectedFour.add("four");
        expectedFour.add(4);
        g.V(777).properties("four").value().forEachRemaining(expectedFour::remove);
        Assert.assertTrue(expectedFour.isEmpty());
        Assert.assertEquals("five", g.V(777).properties("five").value().next());
        Assert.assertEquals("six", g.V(777).properties("six").value().next());
    }

    @Test
    public void testOnCreateEccentricParams() {
        final GraphTraversalSource g = SETUP_GRAPH.traversal();
        g.mergeV(Map.of(T.id, 777))
                .option(Merge.onCreate, Map.of(
                        "one", VertexProperty.Cardinality.list("one"),
                        "two", VertexProperty.Cardinality.set("two"),
                        "three", VertexProperty.Cardinality.single("three")
                )).iterate();
        Assert.assertEquals(3, (long) g.V(777).properties().count().next());
        final Set<Object> expected = new HashSet<>();
        expected.add("one");
        expected.add("two");
        expected.add("three");
        g.V(777).properties().key().forEachRemaining(expected::remove);
        Assert.assertTrue(expected.isEmpty());
        Assert.assertEquals("one", g.V(777).properties("one").value().next());
        Assert.assertEquals("two", g.V(777).properties("two").value().next());
        Assert.assertEquals("three", g.V(777).properties("three").value().next());
    }

    @Test
    public void testMultiplePropertyMatch() {
        final GraphTraversalSource g = SETUP_GRAPH.traversal();
        for (int i = 0; i < 3; i++) {
            g.mergeV(Map.of("name", "Valentyn", "age", 12)).iterate();
        }
        Assert.assertEquals(1, (long) g.V().count().next());
        Assert.assertEquals("Valentyn", g.V().properties("name").value().next());
        Assert.assertEquals(12, g.V().properties("age").value().next());
    }
}
