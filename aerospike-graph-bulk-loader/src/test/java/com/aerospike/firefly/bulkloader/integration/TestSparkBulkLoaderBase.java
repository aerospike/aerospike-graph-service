package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.bulkloader.SparkBulkLoader;
import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.BAD_EDGE_COUNT_EXCEEDED;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.BAD_ENTRY_COUNT_EXCEEDED;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.DATABASE_NOT_EMPTY;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.DUPLICATE_VERTEX_ID_COUNT_EXCEEDED;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.JOB_ALREADY_RUNNING;

public abstract class TestSparkBulkLoaderBase {
    // Directories are relative to firefly/firefly-spark-bulk-loader
    private static final String PROVIDED_ID_PROPERTY_NAME = "testIdName";
    private static final String[] DEFAULT_PARAMS= {"-validate_input_data", "-verify_output_data"};
    protected FireflyGraph graph = null;
    static private final String EDGEID_TEST_DIRECTORIES = "src/test/resources/conf/packed/temp";
    static private final String BASE_PROPERTIES = "src/test/resources/conf/base.properties";

    @Before
    public void beforeEach() {
        Configuration config = getTestConfig();
        graph = FireflyGraph.open(config);
        graph.getBaseGraph().dropDatabase(graph, false);
    }

    @AfterClass
    public static void afterClass() {
        try {
            // Clean all Edge ID related temporary files after execution of test suite
            FileUtils.deleteDirectory(new File(EDGEID_TEST_DIRECTORIES));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @After
    public void afterEach(){
        graph.getBaseGraph().dropDatabase(graph, true);
        Configuration config = getTestConfig();
        try {
            final String edgeIDDirectory = config.getString(BulkLoaderConfigHelper.TEMP_DIRECTORY_KEY);
            FileUtils.deleteDirectory(new File(edgeIDDirectory));
        } catch (final IOException ioe) {
            throw new RuntimeException(ioe);
        } catch (final ConfigurationRuntimeException cre) {
            // Do Nothing
        } finally {
            graph.close();
        }
    }


    protected abstract Configuration getTestConfig();

    protected abstract String getDefaultConfig();

    protected abstract String getKeepIdAsPropertyTrueConfig();

    protected abstract String getDefaultConfigArtificialSupernode();

    protected abstract String getKeepIdAsPropertyTrueConfigArtificialSupernode();

    protected abstract String getPreflightCheckVertex();

    protected abstract String getPreflightCheckEdge();

    protected abstract String getBadEntries();

    protected abstract String getNoIdEdges();

    protected abstract String getNoIdEdgesKeepIdAsPropertyOff();

    protected abstract String getDuplicateVertexId();

    protected abstract String getDuplicateEdgeId();

    protected abstract String getS3FileSystem();

    protected abstract String getGcsFileSystem();
    protected abstract String getFailingClient();
    protected abstract String getHasBadEdges();
    protected abstract String getSamplingSupernode();
    protected abstract String getSamplingSupernodeTooHigh();
    protected abstract String getSamplingSupernodeTooLow();

    @Test
    public void testDataAccuracy() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
        testEdges();
        testVertices();
        testVertexEdgeConnections();
    }

    @Test
    public void testDefault() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));

        final GraphTraversalSource g = graph.traversal();
        final Edge e = g.V().has("name", "Simon").outE("drives").next();
        final Property providedId = e.property(PROVIDED_ID_PROPERTY_NAME);
        Assert.assertFalse(providedId.isPresent());
    }

    @Test
    public void testProvidedEdgeIdPropertyName() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getKeepIdAsPropertyTrueConfig()}, DEFAULT_PARAMS));
        final GraphTraversalSource g = graph.traversal();
        final Edge e = g.V().has("name", "Simon").outE("drives").next();
        Property providedId = e.property("~providedId");
        Assert.assertFalse(providedId.isPresent());
        providedId = e.property(PROVIDED_ID_PROPERTY_NAME);
        Assert.assertEquals("11", providedId.value());
    }

    @Test
    public void testDataAccuracyArtificialSupernodes() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfigArtificialSupernode()}, DEFAULT_PARAMS));
        testEdges();
        testVertices();
        testVertexEdgeConnections();
        testSupernodes();
    }

    @Test
    public void testArtificialSupernodes() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfigArtificialSupernode()},DEFAULT_PARAMS));
        final GraphTraversalSource g = graph.traversal();
        final Edge e = g.V().has("name", "Simon").outE("drives").next();
        final Property providedId = e.property(PROVIDED_ID_PROPERTY_NAME);
        Assert.assertFalse(providedId.isPresent());
        testSupernodes();
    }

    @Test
    public void testProvidedEdgeIdPropertyNameArtificialSupernodes() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getKeepIdAsPropertyTrueConfigArtificialSupernode()}, DEFAULT_PARAMS));
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
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
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

    @Test
    public void testPreflightCheckVertexNotAllowed() {
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-abe", "0", "-c", getPreflightCheckVertex()}, DEFAULT_PARAMS));
            Assert.fail("Bad Vertex entries were not allowed but did not fail.");
        } catch (final RuntimeException e) {
            Assert.assertEquals(BAD_ENTRY_COUNT_EXCEEDED, e.getMessage());
        }
        final GraphTraversalSource g = graph.traversal();
        Assert.assertFalse(g.V().hasNext());
        Assert.assertFalse(g.E().hasNext());
    }

    @Test
    public void testPreflightCheckEdgeNotAllowed() {
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-abe", "0", "-c", getPreflightCheckEdge()}, DEFAULT_PARAMS));
            Assert.fail("Bad Edge entries were not allowed but did not fail.");
        } catch (final RuntimeException e) {
            Assert.assertEquals(BAD_ENTRY_COUNT_EXCEEDED, e.getMessage());
        }
        final GraphTraversalSource g = graph.traversal();
        Assert.assertFalse(g.V().hasNext());
        Assert.assertFalse(g.E().hasNext());
    }

    @Test
    public void testAllowedBadEntryCount() {
        // This data set has 2 bad Vertices, and 1 bad Edge.
        try {
            // Test failing on Vertex
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-abe", "1", "-c", getBadEntries()}, DEFAULT_PARAMS));
            Assert.fail("Bad Edge Vertex were not allowed but did not fail.");
        } catch (final RuntimeException e) {
            Assert.assertEquals(BAD_ENTRY_COUNT_EXCEEDED, e.getMessage());
        }
        final GraphTraversalSource g = graph.traversal();
        Assert.assertFalse(g.V().hasNext());
        Assert.assertFalse(g.E().hasNext());
        try {
            // Test failing on Edge
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-abe", "2", "-c", getBadEntries()}, DEFAULT_PARAMS));
            Assert.fail("Bad Edge entries were not allowed but did not fail.");
        } catch (final RuntimeException e) {
            Assert.assertEquals(BAD_ENTRY_COUNT_EXCEEDED, e.getMessage());
        }
        Assert.assertFalse(g.V().hasNext());
        Assert.assertFalse(g.E().hasNext());
        // Test success
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-abe", "3", "-c", getBadEntries()}, DEFAULT_PARAMS));
        testEdges();
        testVertices();
        testVertexEdgeConnections();
    }

    @Test
    public void testNoIdEdgesKeepAsProperty() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getNoIdEdges()}, DEFAULT_PARAMS));
        testEdges();
        // There's no ~id to keep as a property so it shouldn't be returned.
        final GraphTraversalSource g = graph.traversal();
        Assert.assertFalse(g.E().has("testIdName").hasNext());
    }

    @Test
    public void testNoIdEdgesKeepAsPropertyOff() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getNoIdEdgesKeepIdAsPropertyOff()}, DEFAULT_PARAMS));
        testEdges();
        final GraphTraversalSource g = graph.traversal();
        Assert.assertFalse(g.E().has("testIdName").hasNext());
    }

    @Test
    public void testDuplicateVertexId() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDuplicateVertexId()}, DEFAULT_PARAMS));
        testEdges();
        testVertices();
        testVertexEdgeConnections();
    }

    @Test
    public void testDuplicateVertexIdNotAllowed() {
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-adv", "0", "-c", getDuplicateVertexId()}, DEFAULT_PARAMS));
            Assert.fail("Duplicate Vertex ID did not fail when it should have.");
        } catch (final Exception e) {
            Assert.assertEquals(DUPLICATE_VERTEX_ID_COUNT_EXCEEDED, e.getMessage());
        }
        graph.getBaseGraph().dropDatabase(graph, false);

        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-adv", "2", "-c", getDuplicateVertexId()}, DEFAULT_PARAMS));
            Assert.fail("Duplicate Vertex ID did not fail when it should have.");
        } catch (final Exception e) {
            Assert.assertEquals(DUPLICATE_VERTEX_ID_COUNT_EXCEEDED, e.getMessage());
        }
    }

    @Test
    public void testDuplicateEdgeId() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDuplicateEdgeId(), "-read_only"}, DEFAULT_PARAMS));
        testVertices();
        testVertexEdgeConnections();
        final GraphTraversalSource g = graph.traversal();
        Assert.assertEquals(3, (long) g.E().has("testIdName", "duplicate").count().next());
    }

    @Ignore("TODO GRAPH-888: NPE caused by org.codehaus.groovy.reflection.ReflectionUtils.VM_PLUGIN is null on CI machine")
    @Test
    public void testS3FileSystem() {
        SparkBulkLoader.main(ArrayUtils.addAll(
                new String[]{"-local", "-c", getS3FileSystem(), "-u", System.getenv("AWS_ACCESS_KEY_ID"),
                        "-p", System.getenv("AWS_SECRET_ACCESS_KEY"), "-read_only"},
                DEFAULT_PARAMS));
        testEdges();
        testVertices();
        testVertexEdgeConnections();
    }

    @Test
    public void testGcsFileSystem() {
        SparkBulkLoader.main(ArrayUtils.addAll(
                new String[]{"-local", "-c", getGcsFileSystem(), "-u", System.getenv("GCS_PRIVATE_KEY_ID"),
                        "-p", System.getenv("GCS_PRIVATE_KEY"), "-gem", System.getenv("GCS_CLIENT_EMAIL"),
                        "-read_only"},
                DEFAULT_PARAMS));
        testEdges();
        testVertices();
        testVertexEdgeConnections();
    }

    @Test
    public void testGcsFileSystemMissingUser() {
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(
                    new String[]{"-local", "-c", getGcsFileSystem(), "-p", System.getenv("GCS_PRIVATE_KEY"), "-gem",
                            System.getenv("GCS_CLIENT_EMAIL")},
                    DEFAULT_PARAMS));
                Assert.fail("No user for GCS mode should fail.");
        } catch (final RuntimeException e) {
            Assert.assertEquals("Either 'aerospike.graphloader.gcs-keyfile' or all of 'aerospike.graphloader.gcs-email', 'aerospike.graphloader.remote-user', and 'aerospike.graphloader.remote-passkey' must be specified to read from GCS.", e.getMessage());
        }
    }

    @Test
    public void testGcsFileSystemMissingPasskey() {
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(
                    new String[]{"-local", "-c", getGcsFileSystem(), "-u", System.getenv("GCS_PRIVATE_KEY_ID"),
                            "-gem", System.getenv("GCS_CLIENT_EMAIL")},
                    DEFAULT_PARAMS));
            Assert.fail("No passkey for GCS mode should fail.");
        } catch (final RuntimeException e) {
            Assert.assertEquals("Either 'aerospike.graphloader.gcs-keyfile' or all of 'aerospike.graphloader.gcs-email', 'aerospike.graphloader.remote-user', and 'aerospike.graphloader.remote-passkey' must be specified to read from GCS.", e.getMessage());
        }
    }

    @Test
    public void testGcsFileSystemMissingEmail() {
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(
                    new String[]{"-local", "-c", getGcsFileSystem(), "-u", System.getenv("GCS_PRIVATE_KEY_ID"),
                            "-p", System.getenv("GCS_PRIVATE_KEY")},
                    DEFAULT_PARAMS));
            Assert.fail("No email for GCS mode should fail.");
        } catch (final RuntimeException e) {
            Assert.assertEquals("Either 'aerospike.graphloader.gcs-keyfile' or all of 'aerospike.graphloader.gcs-email', 'aerospike.graphloader.remote-user', and 'aerospike.graphloader.remote-passkey' must be specified to read from GCS.", e.getMessage());
        }
    }

    @Test
    public void testGcsFileSystemKeyFile() {
        SparkBulkLoader.main(ArrayUtils.addAll(
                new String[]{"-local", "-c", getGcsFileSystem(), "-gck", System.getenv("GH_WORKSPACE") + "/gcs-keyfile.json", "-read_only"},
                DEFAULT_PARAMS));
        testEdges();
        testVertices();
        testVertexEdgeConnections();
    }

    @Test
    public void testRetryLogic() {
        // Set this to a really high number in case of failure randomness being really unlucky and triggering the limit
        DatasetOperations.RETRY_LIMIT = Integer.MAX_VALUE;
        // Since this test operates on probabilities run it more than once just to be safe
        final int repeatCount = 3;
        int runCount = 0;
        while (runCount < repeatCount) {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getFailingClient()}, DEFAULT_PARAMS));
            testEdges();
            testVertices();
            testVertexEdgeConnections();
            try (final FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(getDefaultConfig()))) {
                Assert.assertNotEquals(0, (long) graph.traversal().V().count().next());
                Assert.assertNotEquals(0, (long) graph.traversal().E().count().next());
                graph.traversal().V().drop().iterate();
                graph.traversal().E().drop().iterate();
                Assert.assertEquals(0, (long) graph.traversal().V().count().next());
                Assert.assertEquals(0, (long) graph.traversal().E().count().next());
            }
            runCount++;
        }
    }

    @Test
    public void testAllowDetachedEdges() {
        SparkBulkLoader.main(ArrayUtils.addAll(
                new String[]{"-local", "-c", getHasBadEdges()}, DEFAULT_PARAMS));
        testEdges();
        testVertices();
        testVertexEdgeConnections();
    }

    @Test
    public void testNotAllowDetachedEdges() {
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(
                    new String[]{"-local", "-ade", "0", "-c", getHasBadEdges()}, DEFAULT_PARAMS));
            Assert.fail("Detached Edge did not fail bulk load when it should have.");
        } catch (final Exception e) {
            Assert.assertTrue(e.getCause() instanceof RuntimeException);
            Assert.assertEquals(BAD_EDGE_COUNT_EXCEEDED, e.getCause().getMessage());
        }
        graph.getBaseGraph().dropDatabase(graph, false);
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(
                    new String[]{"-local", "-ade", "2", "-c", getHasBadEdges()}, DEFAULT_PARAMS));
            Assert.fail("Detached Edge did not fail bulk load when it should have.");
        } catch (final Exception e) {
            Assert.assertTrue(e.getCause() instanceof RuntimeException);
            Assert.assertEquals(BAD_EDGE_COUNT_EXCEEDED, e.getCause().getMessage());
        }
    }

    @Test
    public void testSupernodeSampling() {
        SparkBulkLoader.main(ArrayUtils.addAll(
                new String[]{"-local", "-ade", "0", "-c", getSamplingSupernode()}, DEFAULT_PARAMS));
    }

    @Test
    public void testSupernodeSamplingTooHigh() {
        try {
        SparkBulkLoader.main(ArrayUtils.addAll(
                new String[]{"-local", "-ade", "0", "-c", getSamplingSupernodeTooHigh()}, DEFAULT_PARAMS));
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Configuration 'aerospike.graphloader.supernode.sampling-percentage' must be between 100 and 0.1 but was"));
        }
    }

    @Test
    public void testSupernodeSamplingTooLow() {
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(
                    new String[]{"-local", "-ade", "0", "-c", getSamplingSupernodeTooLow()}, DEFAULT_PARAMS));
        } catch (IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Configuration 'aerospike.graphloader.supernode.sampling-percentage' must be between 100 and 0.1 but was"));
        }
    }

    @Test
    public void testDatabaseNotEmpty() throws InterruptedException {
        final GraphTraversalSource g = graph.traversal();
        final Vertex v1 = g.addV("v1").next();
        final Vertex v2 = g.addV("v2").next();
        final Edge edge = g.addE("edge").from(v1).to(v2).next();
        Thread.sleep(5000);

        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
            Assert.fail("Writing to a database with edges did not fail when it should have.");
        } catch (final RuntimeException e) {
            Assert.assertEquals(DATABASE_NOT_EMPTY, e.getMessage());
        }

        g.E(edge.id()).drop();

        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
            Assert.fail("Writing to a database with vertices did not fail when it should have.");
        } catch (final RuntimeException e) {
            Assert.assertEquals(DATABASE_NOT_EMPTY, e.getMessage());
        }

        g.V(v1.id()).drop().iterate();
        g.V(v2.id()).drop().iterate();
        Thread.sleep(5000);

        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
    }

    @Test
    public void testConcurrentBulkLoad() throws InterruptedException {
        final Thread existingLoad = new Thread(() -> SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS)));
        existingLoad.start();
        Thread.sleep(500);
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
            Assert.fail("Starting a bulk load when one was already running did not fail when it should have.");
        } catch (final RuntimeException e) {
            Assert.assertEquals(JOB_ALREADY_RUNNING, e.getMessage());
        }
        existingLoad.join();
        testEdges();
        testVertices();
        testVertexEdgeConnections();
        graph.getBaseGraph().dropDatabase(graph, false);

        // Run it again to ensure the flag reset
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
        testEdges();
        testVertices();
        testVertexEdgeConnections();
    }

    @Test
    public void testConfigValidation() {
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(
                    new String[]{"-local", "-c", BASE_PROPERTIES,
                            "-vd", "src/test/resources/sampledata/vertices",
                            "-td", "src/test/resources/conf/packed/temp/eid1"},
                    DEFAULT_PARAMS));
            Assert.fail("Should fail when no Edge directory provided.");
        } catch (final ConfigurationRuntimeException e) {
            Assert.assertEquals("Invalid configuration provided - invalid value for property: aerospike.graphloader.edges",
                    e.getMessage());
        }

        try {
            SparkBulkLoader.main(ArrayUtils.addAll(
                    new String[]{"-local", "-c", BASE_PROPERTIES,
                            "-ed", "src/test/resources/sampledata/edges",
                            "-td", "src/test/resources/conf/packed/temp/eid1"},
                    DEFAULT_PARAMS));
            Assert.fail("Should fail when no Vertex directory provided.");
        } catch (final ConfigurationRuntimeException e) {
            Assert.assertEquals("Invalid configuration provided - invalid value for property: aerospike.graphloader.vertices",
                    e.getMessage());
        }

        try {
            SparkBulkLoader.main(ArrayUtils.addAll(
                    new String[]{"-local", "-c", BASE_PROPERTIES,
                            "-vd", "src/test/resources/sampledata/vertices",
                            "-ed", "src/test/resources/sampledata/edges"},
                    DEFAULT_PARAMS));
            Assert.fail("Should fail when no temp directory provided.");
        } catch (final ConfigurationRuntimeException e) {
            Assert.assertEquals("Invalid configuration provided - invalid value for property: aerospike.graphloader.temp-directory",
                    e.getMessage());
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
