package com.aerospike.firefly.io;

import com.aerospike.firefly.io.utils.EdgeRecordSizeExceededException;
import com.aerospike.firefly.io.utils.VertexRecordSizeExceededException;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.PHAT_EDGE_SIZE;

public class TestRecordSizeExceeded {
    @Rule
    public TestName testName = new TestName();
    private static final int baseInEdgeCount = 2;
    private static final int baseOutEdgeCount = 3;
    private static final int baseVertexPropertyCount = 4;
    private static final int baseVpPropertyCount = 5;
    private static final int baseEdgePropertyCount = 6;
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph SETUP_GRAPH;
    private FireflyGraph graph;
    private Vertex v1;
    private Vertex v2;
    private Edge e0;

    @BeforeClass
    public static void beforeAll() {
        CONFIG.setProperty(ON_RECORD_ID_LIMIT.toLowerCase(), String.format("%d", Long.MAX_VALUE));
        CONFIG.setProperty(PHAT_EDGE_SIZE.toLowerCase(), String.format("%d", 10));
        SETUP_GRAPH = FireflyGraph.open(CONFIG);
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, true);
    }

    @AfterClass
    public static void afterAll() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, true);
        SETUP_GRAPH.close();
    }

    @Before
    public void beforeEach() {
        System.out.println("===> Running " + testName.getMethodName() + " <===");
        graph = FireflyGraph.open(CONFIG);
        graph.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
        final GraphTraversalSource g = graph.traversal();
        final Vertex v1 = g.addV("v1").next();
        this.v1 = v1;
        final Vertex v2 = g.addV("v2").next();
        this.v2 = v2;
        for (int i = 0; i < baseInEdgeCount; i++) {
            g.addE("base" + i).from(v2).to(v1).iterate();
        }
        for (int i = 0; i < baseOutEdgeCount; i++) {
            g.addE("base" + i).from(v1).to(v2).iterate();
        }
        for (int i = 0; i < baseVertexPropertyCount; i++) {
            g.V().hasLabel("v1").property("base" + i, "base" + i).iterate();
        }
        for (int i = 0; i < baseVpPropertyCount; i++) {
            g.V().hasLabel("v1").properties("base0").property("base" + i, "base" + i).iterate();
        }
        for (int i = 0; i < baseEdgePropertyCount; i++) {
            g.E().hasLabel("base0").property("base" + i, "base" + i).iterate();
        }
        this.e0 = g.E().hasLabel("base0").next();
    }

    @After
    public void afterEach() {
        graph.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
        graph.close();
    }

    @Test
    public void testExceedViaEdgeCache() throws InterruptedException {
        final GraphTraversalSource g = graph.traversal();
        int addedEdges = 0;
        while (true) {
            try {
                g.addE(String.valueOf(addedEdges)).from(v1).to(v2).iterate();
                addedEdges++;
                Thread.sleep(2);
            } catch (final VertexRecordSizeExceededException e) {
                Assert.assertEquals(baseInEdgeCount, e.inEdgeCount);
                Assert.assertEquals(baseOutEdgeCount + addedEdges, e.outEdgeCount);
                Assert.assertEquals(baseVertexPropertyCount, e.vertexPropertyCount);
                Assert.assertEquals(baseVpPropertyCount, e.vpPropertyCount);
                return;
            }
        }
    }

    @Test
    public void testExceedViaAddVertexProperty() throws InterruptedException {
        final GraphTraversalSource g = graph.traversal();
        int addedVertexProperties = 0;
        while (true) {
            try {
                g.V(v1.id()).property("added" + addedVertexProperties, "added" + addedVertexProperties).iterate();
                addedVertexProperties++;
                Thread.sleep(2);
            } catch (final VertexRecordSizeExceededException e) {
                Assert.assertEquals(baseInEdgeCount, e.inEdgeCount);
                Assert.assertEquals(baseOutEdgeCount, e.outEdgeCount);
                Assert.assertEquals(baseVertexPropertyCount + addedVertexProperties, e.vertexPropertyCount);
                Assert.assertEquals(baseVpPropertyCount, e.vpPropertyCount);
                return;
            }
        }
    }

    @Test
    public void testExceedViaAddVpProperty() throws InterruptedException {
        final GraphTraversalSource g = graph.traversal();
        int addedVpProperties = 0;
        final FireflyVertexProperty vp = (FireflyVertexProperty) g.V(v1.id()).properties("base0").next();
        while (true) {
            try {
                vp.property("added" + addedVpProperties, "added" + addedVpProperties);
                addedVpProperties++;
                Thread.sleep(2);
            } catch (final VertexRecordSizeExceededException e) {
                Assert.assertEquals(baseInEdgeCount, e.inEdgeCount);
                Assert.assertEquals(baseOutEdgeCount, e.outEdgeCount);
                Assert.assertEquals(baseVertexPropertyCount, e.vertexPropertyCount);
                Assert.assertEquals(baseVpPropertyCount + addedVpProperties, e.vpPropertyCount);
                return;
            }
        }
    }

    @Test
    public void testExceedEdgeRecord() throws InterruptedException {
        final GraphTraversalSource g = graph.traversal();
        int addedProperties = 0;
        while (true) {
            try {
                g.E(e0.id()).property("added" + addedProperties, "added"+ addedProperties).iterate();
                addedProperties++;
                Thread.sleep(2);
            } catch (final EdgeRecordSizeExceededException e) {
                Assert.assertEquals((baseEdgePropertyCount * 2) + addedProperties, e.propertyCount);
                Assert.assertEquals(baseInEdgeCount + baseOutEdgeCount, e.edgePackCount);
                break;
            }
        }
        try {
            g.addE("exceeder").from(v1).to(v2).iterate();
            Assert.fail("Expected adding Edge to full packed record to fail");
        } catch (final EdgeRecordSizeExceededException e) {
            Assert.assertEquals((baseEdgePropertyCount * 2) + addedProperties, e.propertyCount);
            Assert.assertEquals(baseInEdgeCount + baseOutEdgeCount, e.edgePackCount);
        }
    }
}
