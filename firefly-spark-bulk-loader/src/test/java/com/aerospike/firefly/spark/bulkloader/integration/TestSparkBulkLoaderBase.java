package com.aerospike.firefly.spark.bulkloader.integration;

import com.aerospike.firefly.spark.bulkloader.SparkBulkLoader;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class TestSparkBulkLoaderBase {
    // Directories are relative to firefly/firefly-spark-bulk-loader
    private static final String PROVIDED_ID_PROPERTY_NAME = "testIdName";
    protected FireflyGraph graph = null;

    @Before
    public void beforeEach() {
        Configuration config = getTestConfig();
        graph = FireflyGraph.open(config);
    }

    @After
    public void afterEach() {
        graph.getBaseGraph().dropDatabase(true);
        graph.close();
    }

    protected abstract Configuration getTestConfig();

    protected abstract String getDataModel();

    protected abstract String getDefaultConfig();

    protected abstract String getUseProvidedEdgeIdFalseAndKeepIdFalseConfig();

    protected abstract String getUseProvidedEdgeIdFalseKeepIdAsPropertyTrueConfig();

    @Test
    public void testDataAccuracy() {
        SparkBulkLoader.main(new String[]{getDefaultConfig()});
        testEdges();
        testVertexes();
        testVertexEdgeConnections();
    }

    @Test
    public void testUseProvidedEdgeIdTrue() {
        SparkBulkLoader.main(new String[]{getDefaultConfig()});
        final GraphTraversalSource g = graph.traversal();
        final Edge e = g.V().has("name", "Simon").outE("drives").next();
        Assert.assertEquals(11L, e.id());
        final Property providedId = e.property(PROVIDED_ID_PROPERTY_NAME);
        Assert.assertFalse(providedId.isPresent());
    }

    @Test
    public void testUseProvidedEdgeIdFalse() {
        SparkBulkLoader.main(new String[]{getUseProvidedEdgeIdFalseAndKeepIdFalseConfig()});
        final GraphTraversalSource g = graph.traversal();
        final Edge e = g.V().has("name", "Simon").outE("drives").next();
        Assert.assertNotEquals(11L, e.id());
        final Property providedId = e.property(PROVIDED_ID_PROPERTY_NAME);
        Assert.assertFalse(providedId.isPresent());
    }

    @Test
    public void testProvidedEdgeIdPropertyName() {
        SparkBulkLoader.main(new String[]{getUseProvidedEdgeIdFalseKeepIdAsPropertyTrueConfig()});
        final GraphTraversalSource g = graph.traversal();
        final Edge e = g.V().has("name", "Simon").outE("drives").next();
        Assert.assertNotEquals(11L, e.id());
        Property providedId = e.property("~providedId");
        Assert.assertFalse(providedId.isPresent());
        providedId = e.property(PROVIDED_ID_PROPERTY_NAME);
        Assert.assertEquals("11", providedId.value());
    }

    private void testEdges() {
        final GraphTraversalSource g = graph.traversal();
        testEdgeCount(g);
        testEdgeLabelAndProperty(g);
    }

    private void testEdgeCount(final GraphTraversalSource g) {
        long edgeCount = g.E().count().next();
        Assert.assertEquals(11, edgeCount);
        edgeCount = g.E().hasLabel("drives").count().next();
        Assert.assertEquals(2, edgeCount);
        edgeCount = g.E().hasLabel("worksWith").count().next();
        Assert.assertEquals(6, edgeCount);
        edgeCount = g.E().hasLabel("managedBy").count().next();
        Assert.assertEquals(2, edgeCount);
    }

    private void testEdgeLabelAndProperty(final GraphTraversalSource g) {
        // Data set has a single edge without a label - check that it correctly inserted with default edge label value
        final List<Edge> edges = g.E().hasLabel("edge").toList();
        Assert.assertEquals(1, edges.size());
        // Check that properties on edges are loaded properly
        Assert.assertEquals("hello world", edges.get(0).value("testProperty"));
    }

    private void testVertexes() {
        final GraphTraversalSource g = graph.traversal();
        testVertexCount(g);
        testTypeMappings(g);
    }

    private void testVertexCount(final GraphTraversalSource g) {
        long vertexCount = g.V().count().next();
        Assert.assertEquals(7, vertexCount);
        vertexCount = g.V().hasLabel("person").count().next();
        Assert.assertEquals(4, vertexCount);
        vertexCount = g.V().hasLabel("model").count().next();
        Assert.assertEquals(2, vertexCount);
    }

    private void testTypeMappings(final GraphTraversalSource g) {
        // This also implicitly checks the proper truncation of type specifiers on property names
        final Vertex person = g.V().has("name", "Simon").next();
        Assert.assertEquals("Simon", person.value("name"));
        Assert.assertEquals(28, (int)person.value("age"));
        Assert.assertFalse(person.value("glasses"));
        final List<String> companies = person.value("companies");
        Assert.assertEquals("Apache TinkerPop", companies.get(0));
        Assert.assertEquals("Aerospike", companies.get(1));
        final Vertex model = g.V().has("model", "GR86").next();
        Assert.assertEquals("Toyota", model.value("brand"));
        Assert.assertEquals("GR86", model.value("model"));
        Assert.assertEquals(2023, (long)model.value("year"));
        testDefaultVertexLabelAndProperty(g);
    }

    private void testDefaultVertexLabelAndProperty(final GraphTraversalSource g) {
        // Data set has a single vertex without a label - check that it correctly inserted with default vertex label value
        final List<Vertex> vertexes = g.V().hasLabel("vertex").toList();
        Assert.assertEquals(1, vertexes.size());
        final Vertex v = vertexes.get(0);
        // Check that properties with no type specified default to text
        Assert.assertEquals("hello world", v.value("defaultText"));
        Assert.assertEquals("17", v.value("defaultNumber"));
        Assert.assertEquals("true", v.value("defaultBoolean"));
        // Check invalid type specifiers default to text and include the invalid specifier in the fallback property name
        Assert.assertEquals("42", v.value("invalidType:invalid[]"));
    }

    private void testVertexEdgeConnections() {
        final GraphTraversalSource g = graph.traversal();
        testDrives(g);
        testManagedBy(g);
        testWorksWith(g);
    }

    private void testDrives(final GraphTraversalSource g) {
        List<Vertex> vehicles = g.V().has("name", "Simon").out("drives").toList();
        Assert.assertEquals(1, vehicles.size());
        Assert.assertEquals("GR86", vehicles.get(0).value("model"));
        vehicles = g.V().has("name", "Lyndon").out("drives").toList();
        Assert.assertEquals(1, vehicles.size());
        Assert.assertEquals("F150", vehicles.get(0).value("model"));
        // Check there's no writes in the wrong direction
        vehicles = g.V().has("name", "Simon").in("drives").toList();
        Assert.assertEquals(0, vehicles.size());
        vehicles = g.V().has("name", "Lyndon").in("drives").toList();
        Assert.assertEquals(0, vehicles.size());
    }

    private void testManagedBy(final GraphTraversalSource g) {
        List<Vertex> managers = g.V().has("name", "Simon").out("managedBy").toList();
        Assert.assertEquals(1, managers.size());
        Assert.assertEquals("Joe", managers.get(0).value("name"));
        managers = g.V().has("name", "Lyndon").out("managedBy").toList();
        Assert.assertEquals(1, managers.size());
        Assert.assertEquals("Joe", managers.get(0).value("name"));
        // Check vertex IN edge listings
        final Set<Vertex> peons = g.V().has("name", "Joe").in("managedBy").toSet();
        Assert.assertEquals(2, peons.size());
        final Set<String> peonNames = peons.stream().map(v -> (String)v.value("name")).collect(Collectors.toSet());
        Assert.assertTrue((peonNames.contains("Simon") && peonNames.contains("Lyndon")));

    }

    private void testWorksWith(final GraphTraversalSource g) {
        List<Vertex> vehicles = g.V().has("name", "Simon").out("drives").toList();
        Assert.assertEquals(1, vehicles.size());
        Assert.assertEquals("GR86", vehicles.get(0).value("model"));
        vehicles = g.V().has("name", "Lyndon").out("drives").toList();
        Assert.assertEquals(1, vehicles.size());
        Assert.assertEquals("F150", vehicles.get(0).value("model"));
        // Check there's no writes in the wrong direction
        vehicles = g.V().has("name", "Simon").in("drives").toList();
        Assert.assertEquals(0, vehicles.size());
        vehicles = g.V().has("name", "Lyndon").in("drives").toList();
        Assert.assertEquals(0, vehicles.size());
    }
}
