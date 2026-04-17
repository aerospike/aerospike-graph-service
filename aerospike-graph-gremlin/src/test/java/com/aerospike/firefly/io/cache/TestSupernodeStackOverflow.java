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

package com.aerospike.firefly.io.cache;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.FIREFLY_READ_THROUGH_CACHE_WEIGHT;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.READ_SOCKET_TIMEOUT;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.READ_TOTAL_TIMEOUT;

public class TestSupernodeStackOverflow {
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph GRAPH;

    @BeforeClass
    static public void beforeAll() throws InterruptedException {
        CONFIG.setProperty(READ_TOTAL_TIMEOUT, 1500);
        CONFIG.setProperty(READ_SOCKET_TIMEOUT, 500);
        CONFIG.setProperty(FIREFLY_READ_THROUGH_CACHE_WEIGHT, "10000");
        GRAPH = FireflyGraph.open(CONFIG);
        GRAPH.getBaseGraph().dropDatabase(GRAPH, false);
        final var g = GRAPH.traversal();
        final Vertex[] vertices = new Vertex[6];
        for (int i = 0; i < 6; i++) {
            vertices[i] = g.addV("vertex").property(T.id, i).next();
        }
        final AtomicBoolean finished = new AtomicBoolean(false);
        final Random random = new Random();
        for (int i = 0; i < 6; i++) {
            new Thread(() -> {
                while (!finished.get()) {
                    final int id1 = random.nextInt(6);
                    int id2 = id1;
                    while (id2 == id1) {
                        id2 = random.nextInt(6);
                    }
                    try {
                        final String label = id1 == 0 ? "bar" : "foo";
                        g.addE(label).from(vertices[id1]).to(vertices[id2]).iterate();
                    } catch (final Exception ignored) {

                    }
                }
            }).start();
        }
        final List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            final Thread t = new Thread(() -> {
                for (int j = 0; j < 100000; j++) {
                    try {
                        final int id = random.nextInt(5) + 1;
                        g.addE("bar").from(vertices[0]).to(vertices[id]).iterate();
                    } catch (final Exception ignored) {

                    }
                }
            });
            t.start();
            threads.add(t);
        }
        for (final Thread t : threads) {
            t.join();
        }
        finished.set(true);
    }

    @AfterClass
    static public void afterAll() {
        if (GRAPH != null) {
            GRAPH.getBaseGraph().dropDatabase(GRAPH, false);
            GRAPH.close();
        }
    }

    @Test
    public void testStackOverflow() {
        final var g = GRAPH.traversal();
        final List outE = g.V(0).outE("foo").toList();
        Assert.assertTrue(outE.isEmpty());
        final Long count = g.V(0).outE().count().next();
        Assert.assertTrue(count > 600000);
    }
}
