/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.bulkloader.SparkBulkLoader;
import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TimeZone;
import java.util.stream.Collectors;

import static com.aerospike.firefly.bulkloader.integration.util.BulkLoadTestUtil.waitForBulkLoad;
import static com.aerospike.firefly.bulkloader.integration.util.BulkLoadTestUtil.waitForBulkLoadFail;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.BAD_EDGE_COUNT_EXCEEDED;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.BAD_ENTRY_COUNT_EXCEEDED;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.DATABASE_NOT_EMPTY;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.DUPLICATE_VERTEX_ID_COUNT_EXCEEDED;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.JOB_ALREADY_RUNNING;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.getConfig;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class TestSparkBulkLoader {
    // Directories are relative to firefly/firefly-spark-bulk-loader
    private static final String PROVIDED_ID_PROPERTY_NAME = "testIdName";
    private static final String[] DEFAULT_PARAMS = {"-validate_input_data", "-verify_output_data"};
    protected FireflyGraph graph = null;
    static private final String EDGEID_TEST_DIRECTORIES = "src/test/resources/conf/packed/temp";
    static private final String BASE_PROPERTIES = "src/test/resources/conf/base.properties";

    static private final String DEFAULT_CONFIG = "src/test/resources/conf/packed/config.properties";
    static private final String KEEP_ID_AS_PROPERTY_CONFIG = "src/test/resources/conf/packed/keep-provided-id-as-property.properties";
    static private final String DEFAULT_CONFIG_ARTIFICIAL_SUPERNODE = "src/test/resources/conf/packed/config-artificial-supernode.properties";
    static private final String KEEP_ID_AS_PROPERTY_CONFIG_ARTIFICIAL_SUPERNODE = "src/test/resources/conf/packed/keep-provided-id-as-property-artificial-supernode.properties";
    static private final String PREFLIGHT_CHECK_EDGE = "src/test/resources/conf/packed/preflight-check-edge.properties";
    static private final String PREFLIGHT_CHECK_VERTEX = "src/test/resources/conf/packed/preflight-check-vertex.properties";
    static private final String BAD_ENTRIES = "src/test/resources/conf/packed/bad-entries.properties";
    static private final String NO_ID_EDGES = "src/test/resources/conf/packed/no-id-edges.properties";
    static private final String NO_ID_EDGES_KEEP_AS_PROPERTY_OFF = "src/test/resources/conf/packed/no-id-edges-keep-as-property-off.properties";
    static private final String DUPLICATE_VERTEX_ID = "src/test/resources/conf/packed/duplicate-vertex-id.properties";
    static private final String DUPLICATE_EDGE_ID = "src/test/resources/conf/packed/duplicate-edge-id.properties";
    static private final String S3_FILESYSTEM = "src/test/resources/conf/packed/filesystem-s3.properties";
    static private final String GCS_FILESYSTEM = "src/test/resources/conf/packed/filesystem-gcs.properties";
    static private final String FAILING_CLIENT = "src/test/resources/conf/packed/failing-client.properties";
    static private final String DETACHED_EDGES = "src/test/resources/conf/packed/detached-edges.properties";
    static private final String SAMPLE_SUPERNODE = "src/test/resources/conf/packed/config-sampling-supernodes.properties";
    static private final String SAMPLE_SUPERNODE_TOO_HIGH = "src/test/resources/conf/packed/config-sampling-supernodes-too-high.properties";
    static private final String SAMPLE_SUPERNODE_TOO_LOW = "src/test/resources/conf/packed/config-sampling-supernodes-too-low.properties";
    static private final String DATETIME_PROPERTIES = "src/test/resources/conf/packed/config-datetime.properties";
    static private final String BOOLEAN_PROPERTIES = "src/test/resources/conf/packed/bool-parser.properties";
    static private final String MIXED_CARDINALITIES = "src/test/resources/conf/packed/mixed-card.properties";

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
    public void afterEach() {
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

    protected Configuration getTestConfig() {
        return getConfig(Path.of(DEFAULT_CONFIG));
    }

    protected String getDefaultConfig() {
        return DEFAULT_CONFIG;
    }

    protected String getKeepIdAsPropertyTrueConfig() {
        return KEEP_ID_AS_PROPERTY_CONFIG;
    }

    protected String getDefaultConfigArtificialSupernode() {
        return DEFAULT_CONFIG_ARTIFICIAL_SUPERNODE;
    }

    protected String getKeepIdAsPropertyTrueConfigArtificialSupernode() {
        return KEEP_ID_AS_PROPERTY_CONFIG_ARTIFICIAL_SUPERNODE;
    }

    protected String getPreflightCheckEdge() {
        return PREFLIGHT_CHECK_EDGE;
    }

    protected String getBadEntries() {
        return BAD_ENTRIES;
    }

    protected String getPreflightCheckVertex() {
        return PREFLIGHT_CHECK_VERTEX;
    }

    protected String getNoIdEdges() {
        return NO_ID_EDGES;
    }

    protected String getNoIdEdgesKeepIdAsPropertyOff() {
        return NO_ID_EDGES_KEEP_AS_PROPERTY_OFF;
    }

    protected String getDuplicateVertexId() {
        return DUPLICATE_VERTEX_ID;
    }

    protected String getDuplicateEdgeId() {
        return DUPLICATE_EDGE_ID;
    }

    protected String getS3FileSystem() {
        return S3_FILESYSTEM;
    }

    protected String getGcsFileSystem() {
        return GCS_FILESYSTEM;
    }

    protected String getFailingClient() {
        return FAILING_CLIENT;
    }

    protected String getHasBadEdges() {
        return DETACHED_EDGES;
    }

    protected String getSamplingSupernode() {
        return SAMPLE_SUPERNODE;
    }

    protected String getSamplingSupernodeTooHigh() {
        return SAMPLE_SUPERNODE_TOO_HIGH;
    }

    protected String getSamplingSupernodeTooLow() {
        return SAMPLE_SUPERNODE_TOO_LOW;
    }

    protected String getDateTimeProperties() {
        return DATETIME_PROPERTIES;
    }

    protected String getBooleanParseProperties() {
        return BOOLEAN_PROPERTIES;
    }

    protected String getMixedCardinalities() { return MIXED_CARDINALITIES; }

    @Test
    public void testDataAccuracy() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
        waitForBulkLoad(graph.traversal());
        testEdges();
        testVertices();
        testVertexEdgeConnections();
    }

    @Test
    public void testDefault() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
        final GraphTraversalSource g = graph.traversal();
        waitForBulkLoad(g);

        final Edge e = g.V().has("name", "Bob").outE("drives").next();
        final Property providedId = e.property(PROVIDED_ID_PROPERTY_NAME);
        Assert.assertFalse(providedId.isPresent());
    }

    @Test
    public void testProvidedEdgeIdPropertyName() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getKeepIdAsPropertyTrueConfig()}, DEFAULT_PARAMS));
        final GraphTraversalSource g = graph.traversal();
        waitForBulkLoad(g);
        final Edge e = g.V().has("name", "Bob").outE("drives").next();
        Property providedId = e.property("~providedId");
        Assert.assertFalse(providedId.isPresent());
        providedId = e.property(PROVIDED_ID_PROPERTY_NAME);
        Assert.assertEquals("11", providedId.value());
    }

    @Test
    public void testDataAccuracyArtificialSupernodes() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfigArtificialSupernode()}, DEFAULT_PARAMS));
        waitForBulkLoad(graph.traversal());
        testEdges();
        testVertices();
        testVertexEdgeConnections();
        testSupernodes();
    }

    @Test
    public void testArtificialSupernodes() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfigArtificialSupernode()}, DEFAULT_PARAMS));
        final GraphTraversalSource g = graph.traversal();
        waitForBulkLoad(g);
        final Edge e = g.V().has("name", "Bob").outE("drives").next();
        final Property providedId = e.property(PROVIDED_ID_PROPERTY_NAME);
        Assert.assertFalse(providedId.isPresent());
        testSupernodes();
    }

    @Test
    public void testProvidedEdgeIdPropertyNameArtificialSupernodes() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getKeepIdAsPropertyTrueConfigArtificialSupernode()}, DEFAULT_PARAMS));
        final GraphTraversalSource g = graph.traversal();
        waitForBulkLoad(g);
        final Edge e = g.V().has("name", "Bob").outE("drives").next();
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
        waitForBulkLoad(g);
        final Set<Object> expectedIds = Set.of(2L, 3L, 4L, 5L, 6L, 7L, "alice", "carol", "bob", "joe", "GR86", "f150");
        final Set<Object> stringIds = Set.of("2", "3", "4", "5", "6", "7");
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
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-abe", "0", "-c", getPreflightCheckVertex()}, DEFAULT_PARAMS));
        final GraphTraversalSource g = graph.traversal();
        final String e = waitForBulkLoadFail(g);
        Assert.assertEquals(BAD_ENTRY_COUNT_EXCEEDED, e);
        Assert.assertFalse(g.V().hasNext());
        Assert.assertFalse(g.E().hasNext());
    }

    @Test
    public void testPreflightCheckEdgeNotAllowed() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-abe", "0", "-c", getPreflightCheckEdge()}, DEFAULT_PARAMS));
        final GraphTraversalSource g = graph.traversal();
        final String e = waitForBulkLoadFail(g);
        Assert.assertEquals(BAD_ENTRY_COUNT_EXCEEDED, e);
        Assert.assertFalse(g.V().hasNext());
        Assert.assertFalse(g.E().hasNext());
    }

    @Test
    public void testAllowedBadEntryCount() {
        final GraphTraversalSource g = graph.traversal();
        // This data set has 2 bad Vertices, and 1 bad Edge.

        // Test failing on Vertex
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-abe", "2", "-c", getBadEntries()}, DEFAULT_PARAMS));
        String e = waitForBulkLoadFail(g);
        Assert.assertEquals(BAD_ENTRY_COUNT_EXCEEDED, e);

        Assert.assertFalse(g.V().hasNext());
        Assert.assertFalse(g.E().hasNext());

        // Test failing on Edge
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-abe", "4", "-c", getBadEntries()}, DEFAULT_PARAMS));
        e = waitForBulkLoadFail(g);
        Assert.assertEquals(BAD_ENTRY_COUNT_EXCEEDED, e);
        Assert.assertFalse(g.V().hasNext());
        Assert.assertFalse(g.E().hasNext());
        // Test success
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-abe", "5", "-c", getBadEntries()}, DEFAULT_PARAMS));
        waitForBulkLoad(g);
        testEdges();
        testVertices();
        testVertexEdgeConnections();
    }

    @Test
    public void testNoIdEdgesKeepAsProperty() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getNoIdEdges()}, DEFAULT_PARAMS));
        final GraphTraversalSource g = graph.traversal();
        waitForBulkLoad(g);
        testEdges();
        // There's no ~id to keep as a property so it shouldn't be returned.
        Assert.assertFalse(g.E().has("testIdName").hasNext());
    }

    @Test
    public void testNoIdEdgesKeepAsPropertyOff() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getNoIdEdgesKeepIdAsPropertyOff()}, DEFAULT_PARAMS));
        final GraphTraversalSource g = graph.traversal();
        waitForBulkLoad(g);
        testEdges();
        Assert.assertFalse(g.E().has("testIdName").hasNext());
    }

    @Test
    public void testDuplicateVertexId() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDuplicateVertexId()}, DEFAULT_PARAMS));
        waitForBulkLoad(graph.traversal());
        testEdges();
        testVertices();
        testVertexEdgeConnections();
    }

    @Test
    public void testDuplicateVertexIdNotAllowed() {
        final GraphTraversalSource g = graph.traversal();
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-adv", "0", "-c", getDuplicateVertexId()}, DEFAULT_PARAMS));
        String e = waitForBulkLoadFail(g);
        Assert.assertEquals(DUPLICATE_VERTEX_ID_COUNT_EXCEEDED, e);
        graph.getBaseGraph().dropDatabase(graph, false);


        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-adv", "2", "-c", getDuplicateVertexId()}, DEFAULT_PARAMS));
        e = waitForBulkLoadFail(g);
        Assert.assertEquals(DUPLICATE_VERTEX_ID_COUNT_EXCEEDED, e);
    }

    @Test
    public void testDuplicateEdgeId() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDuplicateEdgeId(), "-read_only"}, DEFAULT_PARAMS));
        final GraphTraversalSource g = graph.traversal();
        waitForBulkLoad(g);
        testVertices();
        testVertexEdgeConnections();
        Assert.assertEquals(3, (long) g.E().has("testIdName", "duplicate").count().next());
    }

    @Test
    public void testDatasetWithDateTimeProperties() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDateTimeProperties()}, DEFAULT_PARAMS));
        final GraphTraversalSource g = graph.traversal();
        waitForBulkLoad(g);

        final Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        cal.set(Calendar.YEAR, 2023);
        cal.set(Calendar.MONTH, Calendar.FEBRUARY);
        cal.set(Calendar.DAY_OF_MONTH, 11);
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        final Date date = cal.getTime();

        Vertex vertex = g.V().has("dateOnly", date).next();
        Assert.assertEquals(2007, (long) vertex.value("longProperty"));

        final Calendar calDT = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calDT.set(Calendar.YEAR, 2025);
        calDT.set(Calendar.MONTH, Calendar.JANUARY);
        calDT.set(Calendar.DAY_OF_MONTH, 1);
        calDT.set(Calendar.HOUR_OF_DAY, 17);
        calDT.set(Calendar.MINUTE, 2);
        calDT.set(Calendar.SECOND, 0);
        calDT.set(Calendar.MILLISECOND, 0);
        final Date dateTime = calDT.getTime();

        List<Vertex> vertices = g.V().has("dateTime", dateTime).toList();
        Assert.assertEquals(2, vertices.size());

        Calendar calD1 = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calD1.set(Calendar.YEAR, 2021);
        calD1.set(Calendar.MONTH, Calendar.JANUARY);
        calD1.set(Calendar.DAY_OF_MONTH, 1);
        calD1.set(Calendar.HOUR_OF_DAY, 0);
        calD1.set(Calendar.MINUTE, 0);
        calD1.set(Calendar.SECOND, 0);
        calD1.set(Calendar.MILLISECOND, 0);
        final Date date1 = calD1.getTime();

        final Calendar calD2 = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
        calD2.set(Calendar.YEAR, 2022);
        calD2.set(Calendar.MONTH, Calendar.FEBRUARY);
        calD2.set(Calendar.DAY_OF_MONTH, 2);
        calD2.set(Calendar.HOUR_OF_DAY, 0);
        calD2.set(Calendar.MINUTE, 0);
        calD2.set(Calendar.SECOND, 0);
        calD2.set(Calendar.MILLISECOND, 0);
        final Date date2 = calD2.getTime();

        List<Date> expectedDates = new ArrayList<>();
        expectedDates.add(date1);
        expectedDates.add(date2);
        List<Object> actual = g.V("1").values("dates").toList();
        List<String> actualNormalized = actual.stream().map(Object::toString).collect(Collectors.toList());
        assertThat(actualNormalized, containsInAnyOrder(expectedDates.stream().map(Object::toString).toArray()));

        Edge edge = g.E().has("offsetDateTime", OffsetDateTime.of(2009, 1, 2, 9, 25,
                10, 0, ZoneOffset.UTC)).next();
        Assert.assertEquals(47, (int) edge.value("intProperty"));

        List<OffsetDateTime> expectedODTs = new ArrayList<>();
        expectedODTs.add(OffsetDateTime.of(2025, 5, 7, 4, 33, 24,
                0, ZoneOffset.UTC)); // 2025-05-06T21:33:24-07:00
        expectedODTs.add(OffsetDateTime.of(2024, 6, 1, 18, 11, 15,
                0, ZoneOffset.UTC)); // 2024-06-01T17:11:15-01:00
        expectedODTs.add(OffsetDateTime.of(2025, 1, 2, 10, 30, 0,
                0, ZoneOffset.UTC)); // 2025-01-02T12:30+02:00
        actual = g.V("2").values("offsetDateTimes").toList();
        actualNormalized = actual.stream().map(Object::toString).collect(Collectors.toList());
        assertThat(actualNormalized, containsInAnyOrder(expectedODTs.stream().map(Object::toString).toArray()));
    }

    @Test
    public void testBooleanParsing() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getBooleanParseProperties()}, DEFAULT_PARAMS));
        final GraphTraversalSource g = graph.traversal();
        waitForBulkLoad(g);

        Assert.assertEquals(5, (long) g.V().count().next());
        Assert.assertEquals(3, (long) g.V().has("foo", true).count().next());
        Assert.assertEquals(2, (long) g.V().has("foo", false).count().next());
        Assert.assertEquals(5, (long) g.E().count().next());
        Assert.assertEquals(3, (long) g.E().has("foo", true).count().next());
        Assert.assertEquals(2, (long) g.E().has("foo", false).count().next());
    }

    @Ignore("TODO GRAPH-888: NPE caused by org.codehaus.groovy.reflection.ReflectionUtils.VM_PLUGIN is null on CI machine")
    @Test
    public void testS3FileSystem() {
        SparkBulkLoader.main(ArrayUtils.addAll(
                new String[]{"-local", "-c", getS3FileSystem(), "-u", System.getenv("AWS_ACCESS_KEY_ID"),
                        "-p", System.getenv("AWS_SECRET_ACCESS_KEY"), "-read_only"},
                DEFAULT_PARAMS));
        waitForBulkLoad(graph.traversal());
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
        waitForBulkLoad(graph.traversal());
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
        waitForBulkLoad(graph.traversal());
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
            waitForBulkLoad(graph.traversal());
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
        waitForBulkLoad(graph.traversal());
        testEdges();
        testVertices();
        testVertexEdgeConnections();
    }

    @Test
    public void testNotAllowDetachedEdges() {
        SparkBulkLoader.main(ArrayUtils.addAll(
                new String[]{"-local", "-ade", "0", "-c", getHasBadEdges()}, DEFAULT_PARAMS));
        String e = waitForBulkLoadFail(graph.traversal());
        Assert.assertTrue(e.contains(BAD_EDGE_COUNT_EXCEEDED));
        graph.getBaseGraph().dropDatabase(graph, false);

        SparkBulkLoader.main(ArrayUtils.addAll(
                new String[]{"-local", "-ade", "2", "-c", getHasBadEdges()}, DEFAULT_PARAMS));
        e = waitForBulkLoadFail(graph.traversal());
        Assert.assertTrue(e.contains(BAD_EDGE_COUNT_EXCEEDED));
    }

    @Test
    public void testSupernodeSampling() {
        SparkBulkLoader.main(ArrayUtils.addAll(
                new String[]{"-local", "-ade", "0", "-c", getSamplingSupernode()}, DEFAULT_PARAMS));
        waitForBulkLoad(graph.traversal());
    }

    @Test
    public void testSupernodeSamplingTooHigh() {
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(
                    new String[]{"-local", "-ade", "0", "-c", getSamplingSupernodeTooHigh()}, DEFAULT_PARAMS));
        } catch (final ConfigurationRuntimeException e) {
            Assert.assertEquals("Value provided, \"101\", for configuration key, \"aerospike.graphloader.supernode.sampling-percentage\", is above the maximum acceptable value, \"100\".",
                    e.getMessage());
        }
    }

    @Test
    public void testSupernodeSamplingTooLow() {
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(
                    new String[]{"-local", "-ade", "0", "-c", getSamplingSupernodeTooLow()}, DEFAULT_PARAMS));
        } catch (final ConfigurationRuntimeException e) {
            Assert.assertEquals("Value provided, \"0.001\", for configuration key, \"aerospike.graphloader.supernode.sampling-percentage\", is below the minimum acceptable value, \"0.1\".",
                    e.getMessage());
        }
    }

    @Test
    public void testDatabaseNotEmpty() throws InterruptedException {
        final GraphTraversalSource g = graph.traversal();
        final Vertex v1 = g.addV("v1").next();
        final Vertex v2 = g.addV("v2").next();
        final Edge edge = g.addE("edge").from(v1).to(v2).next();
        Thread.sleep(5000);


        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
        String e = waitForBulkLoadFail(g);
        Assert.assertEquals(DATABASE_NOT_EMPTY, e);

        g.E(edge.id()).drop();

        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
        e = waitForBulkLoadFail(g);
        Assert.assertEquals(DATABASE_NOT_EMPTY, e);

        g.V(v1.id()).drop().iterate();
        g.V(v2.id()).drop().iterate();
        Thread.sleep(5000);

        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
        waitForBulkLoad(g);
    }

    @Test
    public void testConcurrentBulkLoad() throws InterruptedException {
        final Thread existingLoad = new Thread(() -> SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS)));
        existingLoad.start();
        existingLoad.join();
        Thread.sleep(500);
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
            Assert.fail("Starting a bulk load when one was already running did not fail when it should have.");
        } catch (final RuntimeException e) {
            Assert.assertEquals(JOB_ALREADY_RUNNING, e.getMessage());
        }
        waitForBulkLoad(graph.traversal());
        testEdges();
        testVertices();
        testVertexEdgeConnections();
        graph.getBaseGraph().dropDatabase(graph, false);

        // Run it again to ensure the flag reset
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
        waitForBulkLoad(graph.traversal());
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
            Assert.assertEquals("A value for configuration key, \"aerospike.graphloader.edges\", was not provided and is required.",
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
            Assert.assertEquals("A value for configuration key, \"aerospike.graphloader.vertices\", was not provided and is required.",
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
            Assert.assertEquals("A value for configuration key, \"aerospike.graphloader.temp-directory\", was not provided and is required.",
                    e.getMessage());
        }
    }

    @Test
    public void testMixedCardinalityHeaders() {
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getMixedCardinalities()}, DEFAULT_PARAMS));
        final GraphTraversalSource g = graph.traversal();
        waitForBulkLoad(g);
        Assert.assertEquals(12, (long) g.V().count().next());
        Assert.assertEquals(9, (long) g.E().count().next());
        testMixedVertexCardinalityHeader(g, "single", 1);
        testMixedVertexCardinalityHeader(g, "list", 3);
        testMixedVertexCardinalityHeader(g, "set", 2);
        testMixedVertexCardinalityHeader(g, "none", 1);
        testMixedEdgeCardinalityHeader(g, "single");
        testMixedEdgeCardinalityHeader(g, "list");
        testMixedEdgeCardinalityHeader(g, "none");
    }

    private void testMixedVertexCardinalityHeader(final GraphTraversalSource g, final String label, final long count) {
        final long vertexCount = g.V().hasLabel(label).count().next();
        Assert.assertEquals(3, vertexCount);
        var traversal = g.V().hasLabel(label);
        while (traversal.hasNext()) {
            final Vertex v = traversal.next();
            Assert.assertEquals(count, IteratorUtils.count(v.properties()));
        }
    }

    private void testMixedEdgeCardinalityHeader(final GraphTraversalSource g, final String label) {
        final long edgeCount = g.E().hasLabel(label).count().next();
        Assert.assertEquals(3, edgeCount);
        var traversal = g.E().hasLabel(label);
        while (traversal.hasNext()) {
            final Edge e = traversal.next();
            if (label.equals("list")) {
                Assert.assertTrue(e.property("foo").value() instanceof List);
            } else {
                Assert.assertTrue(e.property("foo").value() instanceof String);
            }
        }
    }

    private void testSupernodes() {
        final GraphTraversalSource g = graph.traversal();
        final List<Vertex> vertices = g.V().toList();
        for (final Vertex vertex : vertices) {
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
    }

    private void testEdgeCount(final GraphTraversalSource g) {
        long edgeCount = g.E().count().next();
        Assert.assertEquals(23, edgeCount);
        edgeCount = g.E().hasLabel("drives").count().next();
        Assert.assertEquals(4, edgeCount);
        edgeCount = g.E().hasLabel("worksWith").count().next();
        Assert.assertEquals(12, edgeCount);
        edgeCount = g.E().hasLabel("managedBy").count().next();
        Assert.assertEquals(7, edgeCount);
    }

    private void testVertices() {
        final GraphTraversalSource g = graph.traversal();
        testVertexCount(g);
        testTypeMappings(g);
    }

    private void testVertexCount(final GraphTraversalSource g) {
        long vertexCount = g.V().count().next();
        Assert.assertEquals(12, vertexCount);
        vertexCount = g.V().hasLabel("person").count().next();
        Assert.assertEquals(4, vertexCount);
        vertexCount = g.V().hasLabel("model").count().next();
        Assert.assertEquals(2, vertexCount);
    }

    private void testTypeMappings(final GraphTraversalSource g) {
        // This also implicitly checks the proper truncation of type specifiers on property names
        final Vertex person = g.V().has("name", "Bob").next();
        Assert.assertEquals("Bob", person.value("name"));
        Assert.assertEquals(28, (int) person.value("age"));
        Assert.assertFalse(person.value("glasses"));
        final Iterator<? extends Property<Object>> properties = person.properties("companies");
        final List<String> companies = new ArrayList<>();
        while (properties.hasNext()) {
            companies.add((String) properties.next().value());
        }
        Assert.assertEquals("Apache TinkerPop", companies.get(0));
        Assert.assertEquals("Aerospike", companies.get(1));
        final Vertex model = g.V().has("model", "GR86").next();
        Assert.assertEquals("Toyota", model.value("brand"));
        Assert.assertEquals("GR86", model.value("model"));
        Assert.assertEquals(2023, (long) model.value("year"));
    }

    private void testVertexEdgeConnections() {
        final GraphTraversalSource g = graph.traversal();
        testDrives(g);
        testManagedBy(g);
        testWorksWith(g);
    }

    private void testDrives(final GraphTraversalSource g) {
        List<Vertex> vehicles = g.V().has("name", "Bob").out("drives").toList();
        Assert.assertEquals(1, vehicles.size());
        Assert.assertEquals("GR86", vehicles.get(0).value("model"));
        vehicles = g.V().has("name", "Alice").out("drives").toList();
        Assert.assertEquals(1, vehicles.size());
        Assert.assertEquals("F150", vehicles.get(0).value("model"));
        // Check there's no writes in the wrong direction
        vehicles = g.V().has("name", "Bob").in("drives").toList();
        Assert.assertEquals(0, vehicles.size());
        vehicles = g.V().has("name", "Alice").in("drives").toList();
        Assert.assertEquals(0, vehicles.size());
    }

    private void testManagedBy(final GraphTraversalSource g) {
        List<Vertex> managers = g.V().has("name", "Bob").out("managedBy").toList();
        Assert.assertEquals(1, managers.size());
        Assert.assertEquals("Joe", managers.get(0).value("name"));
        managers = g.V().has("name", "Alice").out("managedBy").toList();
        Assert.assertEquals(1, managers.size());
        Assert.assertEquals("Joe", managers.get(0).value("name"));
        // Check vertex IN edge listings
        final Set<Vertex> peons = g.V().has("name", "Joe").in("managedBy").toSet();
        Assert.assertEquals(2, peons.size());
        final Set<String> peonNames = peons.stream().map(v -> (String) v.value("name")).collect(Collectors.toSet());
        Assert.assertTrue((peonNames.contains("Bob") && peonNames.contains("Alice")));

    }

    private void testWorksWith(final GraphTraversalSource g) {
        List<Vertex> vehicles = g.V().has("name", "Bob").out("drives").toList();
        Assert.assertEquals(1, vehicles.size());
        Assert.assertEquals("GR86", vehicles.get(0).value("model"));
        vehicles = g.V().has("name", "Alice").out("drives").toList();
        Assert.assertEquals(1, vehicles.size());
        Assert.assertEquals("F150", vehicles.get(0).value("model"));
        // Check there's no writes in the wrong direction
        vehicles = g.V().has("name", "Bob").in("drives").toList();
        Assert.assertEquals(0, vehicles.size());
        vehicles = g.V().has("name", "Alice").in("drives").toList();
        Assert.assertEquals(0, vehicles.size());
    }
}
