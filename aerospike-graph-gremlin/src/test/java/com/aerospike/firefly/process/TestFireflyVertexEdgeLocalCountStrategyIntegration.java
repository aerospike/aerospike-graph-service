package com.aerospike.firefly.process;

import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.GLOBAL_EDGE_CACHE_ENABLED;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT;

public class TestFireflyVertexEdgeLocalCountStrategyIntegration {
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph SETUP_GRAPH;

    @BeforeClass
    public static void beforeAll() {
        SETUP_GRAPH = FireflyGraph.open(CONFIG);
    }

    @AfterClass
    public static void afterAll() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
        SETUP_GRAPH.close();
    }

    private static byte[] getEdgeId() {
        final byte[] buffer = new byte[16];
        new Random().nextBytes(buffer);
        return buffer;
    }

    @Before
    public void beforeEach() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
        List<Map.Entry<String, Object>> properties = Collections.singletonList(new AbstractMap.SimpleEntry<>("name", "Simon"));
        final FireflyVertex simon = SETUP_GRAPH.writeVertex(SETUP_GRAPH.getIdFactory().createVertexId(1), "person", properties);
        properties = Collections.singletonList(new AbstractMap.SimpleEntry<>("name", "Lyndon"));
        final FireflyVertex lyndon = SETUP_GRAPH.writeVertex(SETUP_GRAPH.getIdFactory().createVertexId(2), "person", properties);
        properties = Collections.singletonList(new AbstractMap.SimpleEntry<>("name", "Grant"));
        final FireflyVertex grant = SETUP_GRAPH.writeVertex(SETUP_GRAPH.getIdFactory().createVertexId(3), "person", properties);
        properties = Collections.singletonList(new AbstractMap.SimpleEntry<>("name", "Joe"));
        final FireflyVertex joe = SETUP_GRAPH.writeVertex(SETUP_GRAPH.getIdFactory().createVertexId(4), "person", properties);
        SETUP_GRAPH.getOperations().writeEdge(SETUP_GRAPH.getIdFactory().createEdgeId(getEdgeId()), "startedBefore", Collections.emptyList(),
                simon, joe);
        SETUP_GRAPH.getOperations().writeEdge(SETUP_GRAPH.getIdFactory().createEdgeId(getEdgeId()), "startedBefore", Collections.emptyList(),
                lyndon, joe);
        SETUP_GRAPH.getOperations().writeEdge(SETUP_GRAPH.getIdFactory().createEdgeId(getEdgeId()), "startedBefore", Collections.emptyList(),
                grant, joe);
        SETUP_GRAPH.getOperations().writeEdge(SETUP_GRAPH.getIdFactory().createEdgeId(getEdgeId()), "startedBefore", Collections.emptyList(),
                simon, lyndon);
        SETUP_GRAPH.getOperations().writeEdge(SETUP_GRAPH.getIdFactory().createEdgeId(getEdgeId()), "startedBefore", Collections.emptyList(),
                grant, lyndon);
        SETUP_GRAPH.getOperations().writeEdge(SETUP_GRAPH.getIdFactory().createEdgeId(getEdgeId()), "startedBefore", Collections.emptyList(),
                grant, simon);
        SETUP_GRAPH.getOperations().writeEdge(SETUP_GRAPH.getIdFactory().createEdgeId(getEdgeId()), "foo", Collections.emptyList(),
                lyndon, simon);
    }

    @After
    public void afterEach() {
        CONFIG.clearProperty(GLOBAL_EDGE_CACHE_ENABLED.toLowerCase());
        CONFIG.clearProperty(ON_RECORD_ID_LIMIT.toLowerCase());
    }

    @Test
    public void testLocalCountStrategyDefault() {
        // Cache enabled and under cache size
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            assertCountStrategyAccuracy(graph);
            assertCountStrategyVertexLabelAccuracy(graph);
            assertCountStrategyEdgeLabelAccuracy(graph);
        }
    }

    @Test
    public void testLocalCountStrategyEdgeCacheDisabled() {
        // Cache disabled
        CONFIG.setProperty(GLOBAL_EDGE_CACHE_ENABLED.toLowerCase(), "false");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            assertCountStrategyAccuracy(graph);
            assertCountStrategyVertexLabelAccuracy(graph);
            assertCountStrategyEdgeLabelAccuracy(graph);
        }
    }

    @Test
    public void testLocalCountStrategyEdgeCacheSizeExceeded() {
        // Cache size exceeded
        CONFIG.setProperty(ON_RECORD_ID_LIMIT.toLowerCase(), "2");
        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            CONFIG.clearProperty(ON_RECORD_ID_LIMIT.toLowerCase());
            assertCountStrategyAccuracy(graph);
            assertCountStrategyVertexLabelAccuracy(graph);
            assertCountStrategyEdgeLabelAccuracy(graph);
        }
    }

    private void assertCountStrategyAccuracy(final FireflyGraph graph) {
        final GraphTraversalSource g = graph.traversal();
        long totalOutCount = 0;
        var outCount = g.V().local(__.out().count());
        while (outCount.hasNext()) {
            var a = outCount.next();
            totalOutCount += a;
        }
        Assert.assertEquals(7, totalOutCount);

        long totalOutECount = 0;
        var outECount = g.V().local(__.outE().count());
        while (outECount.hasNext()) {
            totalOutECount += outECount.next();
        }
        Assert.assertEquals(7, totalOutECount);

        long totalInCount = 0;
        var inCount = g.V().local(__.in().count());
        while (inCount.hasNext()) {
            totalInCount += inCount.next();
        }
        Assert.assertEquals(7, totalInCount);

        long totalInECount = 0;
        var inECount = g.V().local(__.inE().count());
        while (inECount.hasNext()) {
            totalInECount += inECount.next();
        }
        Assert.assertEquals(7, totalInECount);
    }

    private void assertCountStrategyVertexLabelAccuracy(final FireflyGraph graph) {
        final GraphTraversalSource g = graph.traversal();
        long totalOutCount = 0;
        var outCount = g.V().has("name", "Joe").local(__.out().count());
        while (outCount.hasNext()) {
            totalOutCount += outCount.next();
        }
        Assert.assertEquals(3, totalOutCount);

        long totalOutECount = 0;
        var outECount = g.V().has("name", "Joe").local(__.outE().count());
        while (outECount.hasNext()) {
            totalOutECount += outECount.next();
        }
        Assert.assertEquals(3, totalOutECount);

        long totalInCount = 0;
        var inCount = g.V().has("name", "Joe").local(__.in().count());
        while (inCount.hasNext()) {
            totalInCount += inCount.next();
        }
        Assert.assertEquals(0, totalInCount);

        long totalInECount = 0;
        var inECount = g.V().has("name", "Joe").local(__.inE().count());
        while (inECount.hasNext()) {
            totalInECount += inECount.next();
        }
        Assert.assertEquals(0, totalInECount);
    }

    private void assertCountStrategyEdgeLabelAccuracy(final FireflyGraph graph) {
        final GraphTraversalSource g = graph.traversal();
        long totalOutCount = 0;
        var outCount = g.V().local(__.out("foo").count());
        while (outCount.hasNext()) {
            totalOutCount += outCount.next();
        }
        Assert.assertEquals(1, totalOutCount);

        long totalOutECount = 0;
        var outECount = g.V().local(__.outE("foo").count());
        while (outECount.hasNext()) {
            totalOutECount += outECount.next();
        }
        Assert.assertEquals(1, totalOutECount);

        long totalInCount = 0;
        var inCount = g.V().local(__.in("foo").count());
        while (inCount.hasNext()) {
            totalInCount += inCount.next();
        }
        Assert.assertEquals(1, totalInCount);

        long totalInECount = 0;
        var inECount = g.V().local(__.inE("foo").count());
        while (inECount.hasNext()) {
            totalInECount += inECount.next();
        }
        Assert.assertEquals(1, totalInECount);
    }
}
