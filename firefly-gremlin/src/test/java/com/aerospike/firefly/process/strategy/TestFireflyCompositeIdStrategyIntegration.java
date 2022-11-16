package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedGraph;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyIdFactory;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.IOUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.time.Duration;
import java.time.Instant;
import java.util.AbstractMap;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.Tokens.AIR_ROUTES_50K_URL;
import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ENABLE_COMPOSITE_ID_STRATEGY;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ENABLE_FIREFLY_DROP_STRATEGY;
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
        if (g.V().has("code", "SFO").toList().size() > 0) return;
        SETUP_GRAPH.getBaseGraph().dropDatabase();
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
        // SETUP_GRAPH.getBaseGraph().dropDatabase();
        SETUP_GRAPH.close();
    }

    @After
    public void afterEach() {
        CONFIG.clearProperty(ENABLE_FIREFLY_DROP_STRATEGY.toLowerCase());
    }

    @Test
    public void testCompositeIdStrategyCorrectness() {
        CONFIG.setProperty(ENABLE_COMPOSITE_ID_STRATEGY.toLowerCase(), true);
        final Set<Path> composite_paths;
        final List<Vertex> composite_vertices;
        try (final FireflyGraph graph_composite = FireflyGraph.open(CONFIG)) {
            final GraphTraversalSource g_composite = graph_composite.traversal();
            composite_paths = g_composite.V().has("code", "SFO").out().out().path().toSet();
            composite_vertices = g_composite.V().has("code", "SFO").out().out().toList();
        }

        CONFIG.setProperty(ENABLE_COMPOSITE_ID_STRATEGY.toLowerCase(), false);
        final Set<Path> standard_paths;
        final List<Vertex> standard_vertices;
        try (final FireflyGraph graph_standard = FireflyGraph.open(CONFIG)) {
            final GraphTraversalSource g_standard = graph_standard.traversal();
            standard_paths = g_standard.V().has("code", "SFO").out().out().path().toSet();
            standard_vertices = g_standard.V().has("code", "SFO").out().out().toList();
        }

        Assert.assertEquals(composite_paths, standard_paths);
        composite_vertices.sort(Comparator.comparing(v -> v.id().toString()));
        standard_vertices.sort(Comparator.comparing(v -> v.id().toString()));
        Assert.assertEquals(composite_vertices, standard_vertices);
    }

    @Test
    public void testCompositeIdStrategyPerformance() {
        CONFIG.setProperty(ENABLE_COMPOSITE_ID_STRATEGY.toLowerCase(), true);
        final GraphTraversalSource g_composite;
        final Duration composite_duration_sfo;
        final Duration composite_duration_yyj;
        try (final FireflyGraph graph_composite = FireflyGraph.open(CONFIG)) {
            g_composite = graph_composite.traversal();

            Instant start = Instant.now();
            g_composite.V().has("code", "SFO").out().out().toList();
            composite_duration_sfo = Duration.between(start, Instant.now());

            start = Instant.now();
            g_composite.V().has("code", "YYJ").out().out().toList();
            composite_duration_yyj = Duration.between(start, Instant.now());
        }

        CONFIG.setProperty(ENABLE_COMPOSITE_ID_STRATEGY.toLowerCase(), false);
        final GraphTraversalSource g_standard;
        final Duration standard_duration_sfo;
        final Duration standard_duration_yyj;
        try (final FireflyGraph graph_standard = FireflyGraph.open(CONFIG)) {
            g_standard = graph_standard.traversal();

            Instant start = Instant.now();
            g_standard.V().has("code", "SFO").out().out().toList();
            standard_duration_sfo = Duration.between(start, Instant.now());

            start = Instant.now();
            g_standard.V().has("code", "YYJ").out().out().toList();
            standard_duration_yyj = Duration.between(start, Instant.now());
        }

        System.out.println("Composite Id Strategy sfo: " + composite_duration_sfo.toMillis() + "ms");
        System.out.println("Composite Id Strategy yyj: " + composite_duration_yyj.toMillis() + "ms");
        System.out.println("Standard Id Strategy sfo: " + standard_duration_sfo.toMillis() + "ms");
        System.out.println("Standard Id Strategy yyj: " + standard_duration_yyj.toMillis() + "ms");
    }
}
