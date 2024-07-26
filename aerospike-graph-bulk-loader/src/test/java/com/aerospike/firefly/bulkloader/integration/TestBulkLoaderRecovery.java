package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.bulkloader.SparkBulkLoader;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderState;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateDetectSupernodes;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateReadVertices;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateStart;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateVerifyEdges;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateVerifyVertices;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateWriteEdges;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateWriteVertices;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.getConfig;

public class TestBulkLoaderRecovery {

    private final String DEFAULT_CONFIG = "src/test/resources/conf/packed/config-recovery.properties";

    private Configuration getTestConfig() {
        return getConfig(Path.of(DEFAULT_CONFIG));
    }

    private String getDefaultConfig() {
        return DEFAULT_CONFIG;
    }
    private static final String[] DEFAULT_PARAMS= {"-validate_input_data", "-verify_output_data"};
    protected FireflyGraph graph = null;
    private static long vertexLineCount;
    private static long edgeLineCount;

    @Before
    public void beforeEach() {
        Configuration config = getTestConfig();
        graph = FireflyGraph.open(config);
        graph.traversal().V().drop().iterate();
        System.clearProperty("bulkloader.testing.partition.failure.supernode");
        System.clearProperty("bulkloader.testing.partition.failure.vertex.writing");
        System.clearProperty("bulkloader.testing.partition.failure.vertex.verification");
        System.clearProperty("bulkloader.testing.partition.failure.edge.writing");
        System.clearProperty("bulkloader.testing.partition.failure.edge.verification");
        RecoveryUtil.truncate(graph.getBaseGraph());
    }

    @BeforeClass
    public static void generateData() throws IOException, InterruptedException, ExecutionException {
        final Process python = Runtime.getRuntime().exec("python3 src/test/resources/csv-generate.py");
        if (python.onExit().get().exitValue() != 0) {
            throw new RuntimeException("Failed to generate csv data to run tests.");
        }

        // Count lines.
        final BufferedReader vertexReader =
                new BufferedReader(new FileReader("src/test/resources/recoverydata/vertices/vertexList.csv"));
        final BufferedReader edgeReader =
                new BufferedReader(new FileReader("src/test/resources/recoverydata/edges/edgeList.csv"));

        vertexLineCount = 0;
        while (vertexReader.readLine() != null) vertexLineCount++;
        vertexReader.close();

        edgeLineCount = 0;
        while (edgeReader.readLine() != null) edgeLineCount++;
        edgeReader.close();

        // Remove header.
        vertexLineCount--;
        edgeLineCount--;
    }

    @AfterClass
    public static void clearData() throws IOException {
        Files.deleteIfExists(Path.of("src/test/resources/recoverydata/vertices/vertexList"));
        Files.deleteIfExists(Path.of("src/test/resources/recoverydata/edges/edgeList"));
    }

    @After
    public void afterEach() {
        graph.close();
    }

    @Test
    public void testSupernodeDetectionFailure() {
        System.setProperty("bulkloader.testing.partition.failure.supernode", "true");
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
            Assert.fail("Should have thrown an exception");
        } catch (Exception ignored) {
            // Expected
        }
        System.clearProperty("bulkloader.testing.partition.failure.supernode");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
        Assert.assertEquals(vertexLineCount, graph.traversal().V().count().next().longValue());
        Assert.assertEquals(edgeLineCount, graph.traversal().E().count().next().longValue());

    }

    @Test
    public void testVertexWritingFailure() {
        System.setProperty("bulkloader.testing.partition.failure.vertex.writing", "3");
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
            Assert.fail("Should have thrown an exception");
        } catch (Exception ignored) {
            // Expected
        }
        System.clearProperty("bulkloader.testing.partition.failure.vertex.writing");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
        Assert.assertEquals(vertexLineCount, graph.traversal().V().count().next().longValue());
        Assert.assertEquals(edgeLineCount, graph.traversal().E().count().next().longValue());
    }

    @Test
    public void testVertexVerificationFailure() {
        System.setProperty("bulkloader.testing.partition.failure.vertex.verification", "true");
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
            Assert.fail("Should have thrown an exception");
        } catch (Exception ignored) {
            // Expected
        }
        System.clearProperty("bulkloader.testing.partition.failure.vertex.verification");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
        Assert.assertEquals(vertexLineCount, graph.traversal().V().count().next().longValue());
        Assert.assertEquals(edgeLineCount, graph.traversal().E().count().next().longValue());
    }

    @Test
    public void testEdgeWritingFailure() {
        System.setProperty("bulkloader.testing.partition.failure.edge.writing", "3");
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
            Assert.fail("Should have thrown an exception");
        } catch (Exception ignored) {
            // Expected
        }
        System.clearProperty("bulkloader.testing.partition.failure.edge.writing");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
        Assert.assertEquals(vertexLineCount, graph.traversal().V().count().next().longValue());
        Assert.assertEquals(edgeLineCount, graph.traversal().E().count().next().longValue());
    }

    @Test
    public void testEdgeVerificationFailure() {
        System.setProperty("bulkloader.testing.partition.failure.edge.verification", "true");
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
            Assert.fail("Should have thrown an exception");
        } catch (Exception ignored) {
            // Expected
        }
        System.clearProperty("bulkloader.testing.partition.failure.edge.verification");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
        Assert.assertEquals(vertexLineCount, graph.traversal().V().count().next().longValue());
        Assert.assertEquals(edgeLineCount, graph.traversal().E().count().next().longValue());
    }

    // Test cases:
    // 1. Failure before supernode detection
    // 2. Failure during vertex writing
    // 3. Failure during vertex verification (should not include verification issues)
    // 4. Failure during edge writing
    // 5. Failure during edge verification (should not include verification issues)
}
