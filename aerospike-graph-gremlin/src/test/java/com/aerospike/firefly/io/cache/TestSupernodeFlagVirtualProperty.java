package com.aerospike.firefly.io.cache;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVirtualSupernodeVertexProperty;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestSupernodeFlagVirtualProperty {
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    static {
        CONFIG.setProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, "3");
    }
    private static FireflyGraph INITIALIZATION_GRAPH = null;

    public FireflyGraph graph;

    @BeforeClass
    static public void beforeAll() {
        INITIALIZATION_GRAPH = FireflyGraph.open(CONFIG);
        INITIALIZATION_GRAPH.getBaseGraph().dropDatabase(INITIALIZATION_GRAPH, true);
    }

    @AfterClass
    static public void afterAll() {
        if (INITIALIZATION_GRAPH != null) {
            INITIALIZATION_GRAPH.close();
        }
    }

    @Before
    public void beforeEach() {
        graph = FireflyGraph.open(CONFIG);
        final GraphTraversalSource g = graph.traversal();
        final Vertex v1 = g.addV("v1").property("v11", "1").property("v12", 2).next();
        final Vertex v2 = g.addV("v2").next();
        final Vertex v3 = g.addV("v3").next();
        g.addE("v1Out").from(v1).to(v2).iterate();
        g.addE("v1Out").from(v1).to(v2).iterate();
        g.addE("v1Out").from(v1).to(v3).iterate();
        g.addE("v2Out").from(v2).to(v1).iterate();
        g.addE("v2Out").from(v2).to(v3).iterate();
        g.addE("v3Out").from(v3).to(v1).iterate();
        g.addE("v3Out").from(v3).to(v1).iterate();
    }

    @After
    public void afterEach() {
        graph.getBaseGraph().dropDatabase(graph, true);
        graph.close();
    }

    @Test
    public void testSupernodeHasFilter() {
        final GraphTraversalSource g = graph.traversal();
        var traversal = g.V().has("~supernode");
        Assert.assertEquals(1, IteratorUtils.count(traversal));
        traversal = g.V().hasNot("~supernode");
        Assert.assertEquals(2, IteratorUtils.count(traversal));
    }

    @Test
    public void testSupernodeHasFilterWithValue() {
        final GraphTraversalSource g = graph.traversal();
        var traversal = g.V().has("~supernode", true);
        Assert.assertEquals(0, IteratorUtils.count(traversal));
    }

    @Test
    public void testSupernodePropertyNotReturned() {
        final GraphTraversalSource g = graph.traversal();
        var traversal = g.V().has("~supernode").has("v11");
        var supernode = traversal.next();
        Assert.assertTrue(supernode.property("~supernode") instanceof FireflyVirtualSupernodeVertexProperty);
        Assert.assertEquals(2, IteratorUtils.count(supernode.properties()));
        // It is intentional that ~supernode returns a value when by itself but not when in a list.
        // "property()" propagates down to "properties()" and is used for "has()" checks.
        Assert.assertEquals(1, IteratorUtils.count(supernode.properties("~supernode")));
        Assert.assertEquals(1, IteratorUtils.count(supernode.properties("v11")));
        Assert.assertEquals(2, IteratorUtils.count(supernode.properties("v11", "v12")));
        Assert.assertEquals(1, IteratorUtils.count(supernode.properties("v11", "~supernode")));
        Assert.assertEquals(2, IteratorUtils.count(supernode.properties("v11", "~supernode", "v12")));
    }

    @Test
    public void testSupernodeHasFilterAsFilterStep() {
        final GraphTraversalSource g = graph.traversal();
        var traversal = g.V().hasNot("~supernode").outE();
        Assert.assertEquals(4, IteratorUtils.count(traversal));
    }
}
