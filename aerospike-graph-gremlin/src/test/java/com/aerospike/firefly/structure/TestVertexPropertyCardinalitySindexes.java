package com.aerospike.firefly.structure;

import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestVertexPropertyCardinalitySindexes {
    private static FireflyGraph graph = null;
    private static GraphTraversalSource g = null;

    @BeforeClass
    public static void setUp() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.index.vertex.properties", "name,age");
        graph = FireflyGraph.open(config);
        g = graph.traversal();
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
    public void testVPSindex_MultipleWrittenWithVertex_StringType() {
        final Vertex actualVertex = g.addV("testVPSindex_MultipleWrittenWithVertex_StringType").next();
        actualVertex.property(VertexProperty.Cardinality.list, "name", "Simon");
        actualVertex.property(VertexProperty.Cardinality.list, "name", "Lyndon");

        final Vertex lyndonVertex = g.V().has("name", "Lyndon").next();
        final Vertex simonVertex = g.V().has("name", "Simon").next();
        final Vertex simonLyndonVertex3 = g.V().has("name", "Simon").has("name", "Lyndon").next();
        final TraversalMetrics simonVertexMetrics = g.V().has("name", "Simon").profile().next();
        final TraversalMetrics lyndonVertexMetrics = g.V().has("name", "Lyndon").profile().next();
        final TraversalMetrics simonLyndonVertex3Metrics = g.V().has("name", "Simon").has("name", "Lyndon").profile().next();

        final Metrics simonVertexMetricsFireflyMetric = (Metrics) simonVertexMetrics.getMetrics().toArray()[1];
        Assert.assertEquals(0, simonVertexMetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().size());
        final Metrics lyndonVertexMetricsFireflyMetric = (Metrics) lyndonVertexMetrics.getMetrics().toArray()[1];
        Assert.assertEquals(0, lyndonVertexMetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().size());
        final Metrics simonLyndonVertex3MetricsFireflyMetric = (Metrics) simonLyndonVertex3Metrics.getMetrics().toArray()[1];
        Assert.assertEquals(0, simonLyndonVertex3MetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().size());

        Assert.assertEquals(actualVertex, simonVertex);
        Assert.assertEquals(actualVertex, lyndonVertex);
        Assert.assertEquals(actualVertex, simonLyndonVertex3);
    }

    @Test
    public void testVPSindex_MultipleWrittenWithVertex_DuplicateSingleReturn() {
        final Vertex actualVertex = g.addV("testVPSindex_MultipleWrittenWithVertex_StringType").
                property(VertexProperty.Cardinality.list, "name", "Lyndon").
                property(VertexProperty.Cardinality.list, "name", "Lyndon").
                next();
        final List<Vertex> vertices = g.V().has("name", "Lyndon").toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(actualVertex, vertices.get(0));
        final List<? extends Property<Object>> properties = g.V(actualVertex.id()).properties("name").toList();
        Assert.assertEquals(2, properties.size());
        Assert.assertTrue(properties.stream().allMatch(p -> p.value().equals("Lyndon")));
    }

    @Test
    public void testVPSindex_MultipleAppendedToVertex_DuplicateSingleReturn() {
        final Vertex actualVertex = g.addV("testVPSindex_MultipleWrittenWithVertex_StringType").
                property(VertexProperty.Cardinality.list, "name", "Lyndon").
                property(VertexProperty.Cardinality.list, "name", "Lyndon").
                next();
        final List<Vertex> vertices = g.V().has("name", "Lyndon").toList();
        Assert.assertEquals(1, vertices.size());
        Assert.assertEquals(actualVertex, vertices.get(0));
        final List<? extends Property<Object>> properties = g.V(actualVertex.id()).properties("name").toList();
        Assert.assertEquals(2, properties.size());
        Assert.assertTrue(properties.stream().allMatch(p -> p.value().equals("Lyndon")));
    }
}
