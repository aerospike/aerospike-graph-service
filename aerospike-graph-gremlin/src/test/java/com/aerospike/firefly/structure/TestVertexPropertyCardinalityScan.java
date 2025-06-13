package com.aerospike.firefly.structure;

import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.*;

import java.util.List;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestVertexPropertyCardinalityScan {
    private static FireflyGraph graph = null;
    private static GraphTraversalSource g = null;

    @BeforeClass
    public static void setUp() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        graph = FireflyGraph.open(config);
        g = graph.traversal();
        graph.db.dropDatabase(graph, true);
    }

    @AfterClass
    public static void tearDown() {
        if (graph != null) {
            graph.db.dropDatabase(graph, true);
            graph.close();
        }
    }

    @Before
    public void before() {
        g.V().drop().iterate();
    }

    @Test
    public void testVPScan_MultipleWrittenWithVertex_StringType() {
        final Vertex actualVertex = g.addV("testVPScan_MultipleWrittenWithVertex_StringType").
                property(VertexProperty.Cardinality.list, "name", "Lyndon").
                property(VertexProperty.Cardinality.list, "name", "Simon").next();

        final Vertex lyndonVertex = g.V().has("name", "Lyndon").next();
        final Vertex simonVertex = g.V().has("name", "Simon").next();
        final Vertex simonLyndonVertex1 = g.V().has("name", P.within("Simon", "Lyndon")).next();
        final Vertex simonLyndonVertex2 = g.V().has("name", P.within("Lyndon", "Simon")).next();
        final Vertex simonLyndonVertex3 = g.V().has("name", "Simon").has("name", "Lyndon").next();
        final TraversalMetrics simonVertexMetrics = g.V().has("name", "Simon").profile().next();
        final Metrics simonVertexMetricsFireflyMetric = (Metrics) simonVertexMetrics.getMetrics().toArray()[1];
        Assert.assertFalse(simonVertexMetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().isEmpty());
        final TraversalMetrics lyndonVertexMetrics = g.V().has("name", "Lyndon").profile().next();
        final TraversalMetrics simonLyndonVertex1Metrics = g.V().has("name", P.within("Simon", "Lyndon")).profile().next();
        final TraversalMetrics simonLyndonVertex2Metrics = g.V().has("name", P.within("Lyndon", "Simon")).profile().next();
        final TraversalMetrics simonLyndonVertex3Metrics = g.V().has("name", "Simon").has("name", "Lyndon").profile().next();

        final Metrics lyndonVertexMetricsFireflyMetric = (Metrics) lyndonVertexMetrics.getMetrics().toArray()[1];
        Assert.assertFalse(lyndonVertexMetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().isEmpty());
        final Metrics simonLyndonVertex1MetricsFireflyMetric = (Metrics) simonLyndonVertex1Metrics.getMetrics().toArray()[1];
        Assert.assertFalse(simonLyndonVertex1MetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().isEmpty());
        final Metrics simonLyndonVertex2MetricsFireflyMetric = (Metrics) simonLyndonVertex2Metrics.getMetrics().toArray()[1];
        Assert.assertFalse(simonLyndonVertex2MetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().isEmpty());
        final Metrics simonLyndonVertex3MetricsFireflyMetric = (Metrics) simonLyndonVertex3Metrics.getMetrics().toArray()[1];
        Assert.assertFalse(simonLyndonVertex3MetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().isEmpty());

        Assert.assertEquals(actualVertex, simonVertex);
        Assert.assertEquals(actualVertex, lyndonVertex);
        Assert.assertEquals(actualVertex, simonLyndonVertex1);
        Assert.assertEquals(actualVertex, simonLyndonVertex2);
        Assert.assertEquals(actualVertex, simonLyndonVertex3);
    }

    @Test
    public void testVPSindex_MultipleWrittenWithVertex_DuplicateSingleReturn() {
        final Vertex actualVertex = g.addV("testVPSindex_MultipleWrittenWithVertex_StringType").
                property(VertexProperty.Cardinality.list, "name", "Lyndon").
                property(VertexProperty.Cardinality.list, "name", "Lyndon").next();

        final List<Vertex> vertices = g.V().has("name", "Lyndon").toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(actualVertex, vertices.get(0));
        final List<? extends Property> properties = g.V(actualVertex.id()).properties("name").toList();
        Assert.assertEquals(2, properties.size());
        Assert.assertTrue(properties.stream().allMatch(p -> p.value().equals("Lyndon")));

        final List<Vertex> lyndonVertex = g.V().has("name", "Lyndon").toList();
        final List<Vertex> lyndonVertex1 = g.V().has("name", P.within("Lyndon", "Simon")).toList();
        Assert.assertEquals(1, lyndonVertex.size());
        Assert.assertEquals(1, lyndonVertex1.size());
        Assert.assertEquals(actualVertex, lyndonVertex.get(0));
        Assert.assertEquals(actualVertex, lyndonVertex1.get(0));

        final TraversalMetrics vertexMetrics = g.V().has("name", "Lyndon").profile().next();
        final TraversalMetrics vertexMetrics2 = g.V().has("name", P.within("Lyndon", "Simon")).profile().next();
        final Metrics vertexMetricsFireflyMetric = (Metrics) vertexMetrics.getMetrics().toArray()[1];
        final Metrics vertexMetrics2FireflyMetric = (Metrics) vertexMetrics2.getMetrics().toArray()[1];
        Assert.assertNotEquals(0, vertexMetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().size());
        Assert.assertNotEquals(0, vertexMetrics2FireflyMetric.getNested("FireflyMetrics").getAnnotations().size());
    }
}
