package com.aerospike.firefly.io.cache;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.ConcurrentScanRecordSequenceListener;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIterator;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import groovy.transform.builder.InitializerStrategy;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestVertexPropertyCacheIntegration {
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph SETUP_GRAPH;

    @BeforeClass
    public static void beforeAll() {
        SETUP_GRAPH = CacheTestsUtils.getCacheEnabledAdjacencyDisabledFirefly(CONFIG);
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
    }

    @AfterClass
    public static void afterAll() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
    }

    @Before
    public void beforeEach() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
    }

    @Test
    public void testCacheEnabledAdjacencyDisabled() {
        try (final FireflyGraph graph = CacheTestsUtils.getCacheEnabledAdjacencyDisabledFirefly(CONFIG)) {
            assertAddAndDropVertexProperties(graph);
            assertDropVertexDropsProperties(graph);
        }
    }

    @Test
    public void testEdgeCacheDisabledAdjacencyEnabled() {
        try (final FireflyGraph graph = CacheTestsUtils.getCacheDisabledAdjacencyEnabledFirefly(CONFIG)) {
            assertAddAndDropVertexProperties(graph);
            assertDropVertexDropsProperties(graph);
        }
    }

    @Test
    public void testCacheSizeExceeded() {
        try (final FireflyGraph graph = CacheTestsUtils.getCacheWithSizeFirefly(CONFIG, 2)) {
            assertAddAndDropVertexProperties(graph);
            assertDropVertexDropsProperties(graph);
        }
    }

    private void assertAddAndDropVertexProperties(final FireflyGraph graph) {
        final GraphTraversalSource g = graph.traversal();

        g.addV("cat").property("name", "Vincent").iterate();
        Assert.assertTrue(g.V().hasLabel("cat").hasNext());

        g.V().hasLabel("cat").property("legs", "four").iterate();
        Assert.assertTrue(g.V().has("legs", "four").hasNext());

        g.V().hasLabel("cat").property("tail", "one").iterate();
        Assert.assertTrue(g.V().has("legs", "four").hasNext());
        Assert.assertTrue(g.V().has("tail", "one").hasNext());

        g.V().hasLabel("cat").property("eyes", "two").iterate();
        Assert.assertTrue(g.V().has("legs", "four").hasNext());
        Assert.assertTrue(g.V().has("tail", "one").hasNext());
        Assert.assertTrue(g.V().has("eyes", "two").hasNext());

        g.V().hasLabel("cat").properties("eyes").drop().iterate();
        Assert.assertTrue(g.V().has("legs", "four").hasNext());
        Assert.assertTrue(g.V().has("tail", "one").hasNext());
        Assert.assertFalse(g.V().has("eyes", "two").hasNext());

        g.V().hasLabel("cat").properties("tail").drop().iterate();
        Assert.assertTrue(g.V().has("legs", "four").hasNext());
        Assert.assertFalse(g.V().has("tail", "one").hasNext());
        Assert.assertFalse(g.V().has("eyes", "two").hasNext());

        g.V().hasLabel("cat").properties("legs").drop().iterate();
        Assert.assertFalse(g.V().has("legs", "four").hasNext());
        Assert.assertFalse(g.V().has("tail", "one").hasNext());
        Assert.assertFalse(g.V().has("eyes", "two").hasNext());
    }

    private void assertDropVertexDropsProperties(final FireflyGraph graph) {
        graph.getBaseGraph().dropDatabase(graph, false);
        final GraphTraversalSource g = graph.traversal();

        g.addV("cat").property("name", "Vincent").iterate();
        g.V().hasLabel("cat").property("legs", "four").iterate();
        g.V().hasLabel("cat").property("tail", "one").iterate();
        g.V().hasLabel("cat").property("eyes", "two").iterate();

        g.V().hasLabel("cat").drop().iterate();

        final AerospikeConnection connection = graph.getBaseGraph();
        final Iterator<KeyRecord> properties = scanVPSet(connection);
        Assert.assertFalse(properties.hasNext());
    }

    private Iterator<KeyRecord> scanVPSet(final AerospikeConnection connection) {
        final Monitor scanMonitor = new Monitor();
        final ScanPolicy policy = new ScanPolicy();
        final AerospikeClient client = connection.getClient();
        final ConcurrentScanRecordSequenceListener listener = new ConcurrentScanRecordSequenceListener(scanMonitor,
                Integer.parseInt(ConfigurationHelper.getOrDefault(ConfigurationHelper.Keys.SCAN_MAX_WAIT, connection.conf)));
        client.scanAll(connection.getEventLoops().next(), listener, policy, connection.getNamespace(), connection.VERTEX_PROPERTY_AERO_SET);
        return new FireflyCloseableIterator<>(listener);
    }
}
