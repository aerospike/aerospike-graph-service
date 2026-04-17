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
import com.aerospike.firefly.util.IOUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Comparator;
import java.util.List;
import java.util.Set;

import static com.aerospike.firefly.Tokens.AIR_ROUTES_50K_URL;
import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.ENABLE_COMPOSITE_ID_STRATEGY;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.ENABLE_FIREFLY_DROP_STRATEGY;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphml;

public class TestFireflyCompositeIdStrategyIntegration {
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph SETUP_GRAPH;
    private static final File tempFile;
    private static final URL airRoutesUrl;

    static {
        try {
            airRoutesUrl = new URL(AIR_ROUTES_50K_URL);
            tempFile = new File(System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "air-routes50k.graphml");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @BeforeClass
    public static void beforeAll() throws IOException {
        CONFIG.clearProperty(ENABLE_COMPOSITE_ID_STRATEGY.toLowerCase());
        SETUP_GRAPH = FireflyGraph.open(CONFIG);
        final GraphTraversalSource g = SETUP_GRAPH.traversal();
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
        if (!tempFile.exists()) IOUtil.downloadFileFromURL(airRoutesUrl, tempFile);
        g.V().drop().iterate();
        long start = System.currentTimeMillis();
        System.out.println("Loading Air Routes 50k");
        SETUP_GRAPH.io(graphml()).readGraph(tempFile.getAbsolutePath());
        long finish = System.currentTimeMillis();
        long delta = finish - start;
        System.out.printf("Air Routes 50k Load Test: %d milliseconds elapsed%n", delta);
    }

    @AfterClass
    public static void afterAll() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, true);
        SETUP_GRAPH.close();
    }

    @After
    public void afterEach() {
        CONFIG.clearProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase());
    }

    @Test
    public void testCompositeIdStrategyCorrectness() throws IOException {
        final Set<Path> refrence_paths;
        final List<Vertex> refrence_vertices;
        try (final TinkerGraph referenceGraph = TinkerGraph.open()) {
            referenceGraph.io(graphml()).readGraph(tempFile.getAbsolutePath());
            final GraphTraversalSource g_refrence = referenceGraph.traversal();
            refrence_paths = g_refrence.V().has("code", "SFO").out().out().path().toSet();
            refrence_vertices = g_refrence.V().has("code", "SFO").out().out().toList();
        }

        final Set<Path> composite_paths;
        final List<Vertex> composite_vertices;
        try (final FireflyGraph graph_composite = FireflyGraph.open(CONFIG)) {
            final GraphTraversalSource g_composite = graph_composite.traversal();
            composite_paths = g_composite.V().has("code", "SFO").out().out().path().toSet();
            composite_vertices = g_composite.V().has("code", "SFO").out().out().toList();
        }

        Assert.assertEquals(refrence_paths.size(), composite_paths.size());
        Assert.assertEquals(refrence_paths, composite_paths);

        refrence_vertices.sort(Comparator.comparing(v -> v.id().toString()));
        composite_vertices.sort(Comparator.comparing(v -> v.id().toString()));
        Assert.assertEquals(refrence_vertices, composite_vertices);
    }
}
