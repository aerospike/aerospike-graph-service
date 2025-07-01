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
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Calendar;
import java.util.List;
import java.util.NoSuchElementException;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.DateTimeUtil.getDate;

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
    public void testVP_MultiStartingValue_Double() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_MultiStartingValue_Double")
                .property(VertexProperty.Cardinality.list, "value", 1.0)
                .property(VertexProperty.Cardinality.list, "value", 2.0)
                .next();

        final List<Vertex> vertices1 = g.V().has("value", P.within(1.0, 2.0)).toList();
        final List<Vertex> vertices2 = g.V().has("value", 1.0).toList();
        final List<Vertex> vertices3 = g.V().has("value", 2.0).toList();
        final List<Vertex> vertices4 = g.V().has("value", P.gt(0.9)).toList();
        final List<Vertex> vertices5 = g.V().has("value", P.lt(2.1)).toList();
        final List<Vertex> vertices6 = g.V().has("value", P.gte(1.0)).toList();
        final List<Vertex> vertices7 = g.V().has("value", P.lte(2.0)).toList();
        final List<Vertex> vertices8 = g.V().has("value", P.gt(1.5)).toList();
        final List<Vertex> vertices9 = g.V().has("value", P.lt(1.5)).toList();
        final List<Vertex> vertices10 = g.V().has("value", P.lt(1.0)).toList();
        final List<Vertex> vertices11 = g.V().has("value", P.gt(2.0)).toList();

        Assert.assertEquals(1, vertices1.size());
        Assert.assertEquals(1, vertices2.size());
        Assert.assertEquals(1, vertices3.size());
        Assert.assertEquals(1, vertices4.size());
        Assert.assertEquals(1, vertices5.size());
        Assert.assertEquals(1, vertices6.size());
        Assert.assertEquals(1, vertices7.size());
        Assert.assertEquals(1, vertices8.size());
        Assert.assertEquals(1, vertices9.size());
        Assert.assertEquals(0, vertices10.size());
        Assert.assertEquals(0, vertices11.size());

        Assert.assertEquals(v, vertices1.get(0));
        Assert.assertEquals(v, vertices2.get(0));
        Assert.assertEquals(v, vertices3.get(0));
        Assert.assertEquals(v, vertices4.get(0));
        Assert.assertEquals(v, vertices5.get(0));
        Assert.assertEquals(v, vertices6.get(0));
        Assert.assertEquals(v, vertices7.get(0));
        Assert.assertEquals(v, vertices8.get(0));
        Assert.assertEquals(v, vertices9.get(0));
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

    @Test
    public void testVPSindex_MultipleWrittenWithVertex_IntegerType() {
        final Vertex actualVertex = g.addV("testVPSindex_MultipleWrittenWithVertex_StringType").next();
        actualVertex.property(VertexProperty.Cardinality.list, "age", 10);
        actualVertex.property(VertexProperty.Cardinality.list, "age", 31);

        final Vertex lyndonVertex = g.V().has("age", P.lte(50)).next();
        final Vertex simonVertex = g.V().has("age", P.lt(11)).next();
        final Vertex simonVertex1 = g.V().has("age", P.lte(10)).next();
        final Vertex simonVertex2 = g.V().has("age", P.gt(9)).next();
        final Vertex simonVertex3 = g.V().has("age", P.gte(10)).next();
        Assert.assertThrows(NoSuchElementException.class, () -> g.V().has("age", P.gt(31)).next());
        Assert.assertThrows(NoSuchElementException.class, () -> g.V().has("age", P.gte(32)).next());
        Assert.assertThrows(NoSuchElementException.class, () -> g.V().has("age", P.lt(10)).next());
        Assert.assertThrows(NoSuchElementException.class, () -> g.V().has("age", P.lte(9)).next());
    }

    @Test
    public void testVPSindex_DateTimeType() {
        final Vertex actualVertex = g.addV("testVPSindex_DateTimeType").next();
        actualVertex.property(VertexProperty.Cardinality.list, "date", getDate(2024, Calendar.FEBRUARY, 3));
        actualVertex.property(VertexProperty.Cardinality.list, "date", getDate(2019, Calendar.MAY, 27));

        final Vertex firstDateVertex = g.V().has("date", getDate(2024, Calendar.FEBRUARY, 3)).next();
        final Vertex secondDateVertex = g.V().has("date", getDate(2019, Calendar.MAY, 27)).next();
        final Vertex thirdDateVertex = g.V().has("date", P.gt(getDate(2024, Calendar.FEBRUARY, 2))).next();
        final Vertex fourthDateVertex = g.V().has("date", P.gte(getDate(2024, Calendar.FEBRUARY, 3))).next();
        final Vertex fifthDateVertex = g.V().has("date", P.lt(getDate(2024, Calendar.FEBRUARY, 4))).next();
        final Vertex sixthDateVertex = g.V().has("date", P.lte(getDate(2024, Calendar.FEBRUARY, 3))).next();
        final TraversalMetrics firstDateVertexMetrics = g.V().has("date", getDate(2024, Calendar.FEBRUARY, 3)).profile().next();
        final TraversalMetrics secondDateVertexMetrics = g.V().has("date", getDate(2019, Calendar.MAY, 27)).profile().next();
        final TraversalMetrics thirdDateVertexMetrics = g.V().has("date", P.gt(getDate(2024, Calendar.FEBRUARY, 2))).profile().next();
        final TraversalMetrics fourthDateVertexMetrics = g.V().has("date", P.gte(getDate(2024, Calendar.FEBRUARY, 3))).profile().next();
        final TraversalMetrics fifthDateVertexMetrics = g.V().has("date", P.lt(getDate(2024, Calendar.FEBRUARY, 4))).profile().next();
        final TraversalMetrics sixthDateVertexMetrics = g.V().has("date", P.lte(getDate(2024, Calendar.FEBRUARY, 3))).profile().next();

        final Metrics firstDateVertexMetricsFireflyMetric = (Metrics) firstDateVertexMetrics.getMetrics().toArray()[1];
        Assert.assertFalse(firstDateVertexMetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().isEmpty());
        final Metrics secondDateVertexMetricsFireflyMetric = (Metrics) secondDateVertexMetrics.getMetrics().toArray()[1];
        Assert.assertFalse(secondDateVertexMetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().isEmpty());
        final Metrics thirdDateVertexMetricsFireflyMetric = (Metrics) thirdDateVertexMetrics.getMetrics().toArray()[1];
        Assert.assertFalse(thirdDateVertexMetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().isEmpty());
        final Metrics fourthDateVertexMetricsFireflyMetric = (Metrics) fourthDateVertexMetrics.getMetrics().toArray()[1];
        Assert.assertFalse(fourthDateVertexMetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().isEmpty());
        final Metrics fifthDateVertexMetricsFireflyMetric = (Metrics) fifthDateVertexMetrics.getMetrics().toArray()[1];
        Assert.assertFalse(fifthDateVertexMetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().isEmpty());
        final Metrics sixthDateVertexMetricsFireflyMetric = (Metrics) sixthDateVertexMetrics.getMetrics().toArray()[1];
        Assert.assertFalse(sixthDateVertexMetricsFireflyMetric.getNested("FireflyMetrics").getAnnotations().isEmpty());

        Assert.assertEquals(actualVertex, firstDateVertex);
        Assert.assertEquals(actualVertex, secondDateVertex);
        Assert.assertEquals(actualVertex, thirdDateVertex);
        Assert.assertEquals(actualVertex, fourthDateVertex);
        Assert.assertEquals(actualVertex, fifthDateVertex);
        Assert.assertEquals(actualVertex, sixthDateVertex);
    }
}
