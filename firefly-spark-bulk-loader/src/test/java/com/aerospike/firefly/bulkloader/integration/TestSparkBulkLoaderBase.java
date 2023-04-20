package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.bulkloader.SparkBulkLoader;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
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
        graph.getBaseGraph().dropDatabase(graph, true);
        graph.close();
    }

    protected abstract Configuration getTestConfig();

    protected abstract String getDefaultConfig();

    protected abstract String getKeepIdAsPropertyTrueConfig();

    protected abstract String getDefaultConfigArtificialSupernode();

    protected abstract String getKeepIdAsPropertyTrueConfigArtificialSupernode();

    @Test
    public void testDataAccuracy() {
        SparkBulkLoader.main(new String[]{"-m", "local", "-c", getDefaultConfig()});
        testEdges();
        testVertices();
        testVertexEdgeConnections();
    }

    @Test
    public void testDefault() {
        SparkBulkLoader.main(new String[]{"-m", "local", "-c", getDefaultConfig()});
        final GraphTraversalSource g = graph.traversal();
        final Edge e = g.V().has("name", "Simon").outE("drives").next();
        final Property providedId = e.property(PROVIDED_ID_PROPERTY_NAME);
        Assert.assertFalse(providedId.isPresent());
    }

    @Test
    public void testProvidedEdgeIdPropertyName() {
        SparkBulkLoader.main(new String[]{"-m", "local", "-c", getKeepIdAsPropertyTrueConfig()});
        final GraphTraversalSource g = graph.traversal();
        final Edge e = g.V().has("name", "Simon").outE("drives").next();
        Property providedId = e.property("~providedId");
        Assert.assertFalse(providedId.isPresent());
        providedId = e.property(PROVIDED_ID_PROPERTY_NAME);
        Assert.assertEquals("11", providedId.value());
    }

    @Test
    public void testDataAccuracyArtificialSupernodes() {
        SparkBulkLoader.main(new String[]{"-m", "local", "-c", getDefaultConfigArtificialSupernode()});
        testEdges();
        testVertices();
        testVertexEdgeConnections();
        testSupernodes();
    }

    @Test
    public void testArtificialSupernodes() {
        SparkBulkLoader.main(new String[]{"-m", "local", "-c", getDefaultConfigArtificialSupernode()});
        final GraphTraversalSource g = graph.traversal();
        final Edge e = g.V().has("name", "Simon").outE("drives").next();
        final Property providedId = e.property(PROVIDED_ID_PROPERTY_NAME);
        Assert.assertFalse(providedId.isPresent());
        testSupernodes();
    }

    @Test
    public void testProvidedEdgeIdPropertyNameArtificialSupernodes() {
        SparkBulkLoader.main(new String[]{"-m", "local", "-c", getKeepIdAsPropertyTrueConfigArtificialSupernode()});
        final GraphTraversalSource g = graph.traversal();
        final Edge e = g.V().has("name", "Simon").outE("drives").next();
        Property providedId = e.property("~providedId");
        Assert.assertFalse(providedId.isPresent());
        providedId = e.property(PROVIDED_ID_PROPERTY_NAME);
        Assert.assertEquals("11", providedId.value());
        testSupernodes();
    }

    @Test
    public void testVertexIds() {
        SparkBulkLoader.main(new String[]{"-m", "local", "-c", getDefaultConfig()});
        final GraphTraversalSource g = graph.traversal();
        final Set<Object> expectedIds = Set.of(1L, 2L, 3L, 4L, 5L, 6L, 7L, "lyndon", "grant", "simon", "joe", "GR86", "f150");
        final Set<Object> stringIds = Set.of("1", "2", "3", "4", "5", "6", "7");
        final List<Vertex> vertexIds = g.V().toList();
        Assert.assertEquals(expectedIds.size(), vertexIds.size());
        for (final Vertex v : vertexIds) {
            Assert.assertTrue(expectedIds.contains(v.id()));
        }
        for (final Object id : expectedIds) {
            Assert.assertTrue(g.V(id).hasNext());
        }
        for (final Object id : stringIds) {
            Assert.assertTrue(g.V(id).hasNext());
        }
    }

    private void testSupernodes() {
        final GraphTraversalSource g = graph.traversal();
        final List<Vertex> vertices = g.V().toList();
        for (final Vertex vertex: vertices) {
            final FireflyVertex fireflyVertex = (FireflyVertex) vertex;

            // Car models and vertex have <=1 edge in either direction and therefore are not supernodes, all other vertices are.
            if ("model".equals(vertex.label()) || "vertex".equals(vertex.label()) || "modell".equals(vertex.label())) {
                Assert.assertFalse(fireflyVertex.isEdgeCacheOverflowed());
            } else {
                Assert.assertTrue(fireflyVertex.isEdgeCacheOverflowed());
            }
        }
    }

    private void testEdges() {
        final GraphTraversalSource g = graph.traversal();
        testEdgeCount(g);
        testEdgeLabelAndProperty(g);
    }

    private void testEdgeCount(final GraphTraversalSource g) {
        long edgeCount = g.E().count().next();
        Assert.assertEquals(24, edgeCount);
        edgeCount = g.E().hasLabel("drives").count().next();
        Assert.assertEquals(4, edgeCount);
        edgeCount = g.E().hasLabel("worksWith").count().next();
        Assert.assertEquals(12, edgeCount);
        edgeCount = g.E().hasLabel("managedBy").count().next();
        Assert.assertEquals(7, edgeCount);
    }

    private void testEdgeLabelAndProperty(final GraphTraversalSource g) {
        // Data set has a single edge without a label - check that it correctly inserted with default edge label value
        final List<Edge> edges = g.E().hasLabel("edge").toList();
        Assert.assertEquals(1, edges.size());
        final Edge e = edges.get(0);
        // Check that properties on edges are loaded properly
        Assert.assertEquals("hello world", e.value("defaultText"));
        Assert.assertEquals("17", e.value("defaultNumber"));
        Assert.assertEquals("true", e.value("defaultBoolean"));
        // Check invalid type specifiers default to text and include the invalid specifier in the fallback property name
        Assert.assertEquals("42", e.value("invalidType:invalid[]"));
        // Check null properties dont exist
        Assert.assertFalse(g.E().hasLabel("edge").has("nullValue").hasNext());
        Assert.assertFalse(g.E().hasLabel("edge").has("nullInt").hasNext());
        // Check null properties in a list do exist
        final List<String> nullInList = e.value("nullInList");
        Assert.assertEquals(2, nullInList.size());
        Assert.assertNull(nullInList.get(0));
        Assert.assertEquals("secondElement", nullInList.get(1));
        // Check that null properties are not somehow saved as a valid property
        Assert.assertEquals(5, (long) g.E().hasLabel("edge").properties().count().next());
    }

    private void testVertices() {
        final GraphTraversalSource g = graph.traversal();
        testVertexCount(g);
        testTypeMappings(g);
    }

    private void testVertexCount(final GraphTraversalSource g) {
        long vertexCount = g.V().count().next();
        Assert.assertEquals(13, vertexCount);
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
        final List<Vertex> vertices = g.V().hasLabel("vertex").toList();
        Assert.assertEquals(1, vertices.size());
        final Vertex v = vertices.get(0);
        // Check that properties with no type specified default to text
        Assert.assertEquals("hello world", v.value("defaultText"));
        Assert.assertEquals("17", v.value("defaultNumber"));
        Assert.assertEquals("true", v.value("defaultBoolean"));
        // Check invalid type specifiers default to text and include the invalid specifier in the fallback property name
        Assert.assertEquals("42", v.value("invalidType:invalid[]"));
        // Check null properties dont exist
        Assert.assertFalse(g.V().hasLabel("vertex").has("nullValue").hasNext());
        Assert.assertFalse(g.V().hasLabel("vertex").has("nullInt").hasNext());
        // Check null properties in a list do exist
        final List<String> nullInList = v.value("nullInList");
        Assert.assertEquals(2, nullInList.size());
        Assert.assertNull(nullInList.get(0));
        Assert.assertEquals("secondElement", nullInList.get(1));
        // Check that null properties are not somehow saved as a valid property
        Assert.assertEquals(5, (long) g.V().hasLabel("vertex").properties().count().next());
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
