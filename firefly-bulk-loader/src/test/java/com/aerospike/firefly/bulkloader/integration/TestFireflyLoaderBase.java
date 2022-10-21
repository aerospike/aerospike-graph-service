package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.firefly.bulkloader.io.FireflyLoader;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.Tokens;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.EDGE_DIRECTORY_KEY;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.ID_BUFFER_KEY;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.ID_PROPERTY_NAME_KEY;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.USE_PROVIDED_ID_KEY;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.VERTEX_DIRECTORY_KEY;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.AEROSPIKE_HOST;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.AEROSPIKE_NAMESPACE;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.AEROSPIKE_PORT;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.COUNTER;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.FIREFLY_DATA_MODEL;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.Sets.ID_MANAGER_SET;
import static com.aerospike.firefly.util.ConfigurationHelper.getOrDefault;

public abstract class TestFireflyLoaderBase {
    // Directories are relative to firefly/firefly-bulk-loader
    protected static final String TEST_EDGE_DIRECTORY = "src/test/resources/sampledata/edges";
    protected static final String TEST_VERTEX_DIRECTORY = "src/test/resources/sampledata/vertices";
    protected Configuration config;
    protected FireflyGraph graph = null;

    @Before
    public void beforeEach() {
        resetConfig();
        graph = FireflyGraph.open(config);
        graph.getBaseGraph().dropDatabase();
    }

    @After
    public void afterEach() {
        graph.close();
    }

    private void resetConfig() {
        config = getDefaultTestConfig();
        config.setProperty(FIREFLY_DATA_MODEL.toLowerCase(), getDataModel());
        config.setProperty(EDGE_DIRECTORY_KEY, TEST_EDGE_DIRECTORY);
        config.setProperty(VERTEX_DIRECTORY_KEY, TEST_VERTEX_DIRECTORY);
    }

    public Configuration getDefaultTestConfig() {
        return new MapConfiguration(new HashMap<>(){{
            put(AEROSPIKE_HOST.toLowerCase(), "172.17.0.1");
            put(AEROSPIKE_PORT.toLowerCase(), "3000");
            put(AEROSPIKE_NAMESPACE.toLowerCase(), "test");
        }});
    }

    abstract protected String getDataModel();

    @Test
    public void testDataAccuracy() {
        final FireflyLoader bulkLoader = new FireflyLoader(graph, config);
        bulkLoader.load();
        testEdges();
        testVertices();
        testVertexEdgeConnections();
    }

    @Test
    public void testIdBufferConfig() {
        final long bufferSize = 25;
        config.setProperty(ID_BUFFER_KEY, bufferSize);
        final FireflyLoader bulkLoader = new FireflyLoader(graph, config);
        bulkLoader.load();
        final AerospikeConnection connection = graph.getBaseGraph();
        final AerospikeClient client = connection.getClient();
        final String idManagerSet = getOrDefault(ID_MANAGER_SET, config);
        final Key vertexIdKey = new Key(connection.getNamespace(), idManagerSet, Tokens.VERTEX_ID_COUNTER);
        final Record vertexIdRecord = client.get(null, vertexIdKey);
        Assert.assertEquals(-bufferSize, vertexIdRecord.getLong(COUNTER));
        final Key edgeIdKey = new Key(connection.getNamespace(), idManagerSet, Tokens.EDGE_ID_COUNTER);
        final Record edgeIdRecord = client.get(null, edgeIdKey);
        Assert.assertEquals(-bufferSize, edgeIdRecord.getLong(COUNTER));
    }

    @Test
    public void testUseProvidedIdTrue() {
        config.setProperty(USE_PROVIDED_ID_KEY, true);
        final FireflyLoader bulkLoader = new FireflyLoader(graph, config);
        bulkLoader.load();
        final GraphTraversalSource g = graph.traversal();
        final Vertex v = g.V().has("name", "Simon").next();
        Assert.assertEquals(5L, v.id());
        final VertexProperty providedId = v.property("~providedId");
        Assert.assertFalse(providedId.isPresent());
    }

    @Test
    public void testUseProvidedIdFalse() {
        config.setProperty(USE_PROVIDED_ID_KEY, false);
        final FireflyLoader bulkLoader = new FireflyLoader(graph, config);
        bulkLoader.load();
        final GraphTraversalSource g = graph.traversal();
        final Vertex v = g.V().has("name", "Simon").next();
        Assert.assertNotEquals(5L, v.id());
        final VertexProperty providedId = v.property("~providedId");
        Assert.assertTrue(providedId.isPresent());
    }

    @Test
    public void testProvidedIdPropertyNameName() {
        final String idPropertyName = "someAbsurdPropertyNameForTheOldId";
        config.setProperty(USE_PROVIDED_ID_KEY, false);
        config.setProperty(ID_PROPERTY_NAME_KEY, idPropertyName);
        final FireflyLoader bulkLoader = new FireflyLoader(graph, config);
        bulkLoader.load();
        final GraphTraversalSource g = graph.traversal();
        final Vertex v = g.V().has("name", "Simon").next();
        Assert.assertNotEquals(5L, v.id());
        VertexProperty<String> providedId = v.property("~providedId");
        Assert.assertFalse(providedId.isPresent());
        providedId = v.property(idPropertyName);
        Assert.assertEquals("5", providedId.value());
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

    private void testVertices() {
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
        final List<Vertex> vertices = g.V().hasLabel("vertex").toList();
        Assert.assertEquals(1, vertices.size());
        final Vertex v = vertices.get(0);
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
