package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.bulkloader.SparkBulkLoader;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.lang3.ArrayUtils;
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

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.RESUME;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.getConfig;

public class TestBulkLoaderRecovery {

    private final String DEFAULT_CONFIG = "src/test/resources/conf/packed/config-recovery.properties";
    private final String FAIL_EDGE_WRITE = "src/test/resources/conf/packed/config-recovery-fail-edge-write.properties";
    private final String FAIL_EDGE_VERIFY = "src/test/resources/conf/packed/config-recovery-fail-edge-verify.properties";
    private final String FAIL_VERTEX_WRITE = "src/test/resources/conf/packed/config-recovery-fail-vertex-write.properties";
    private final String FAIL_VERTEX_VERIFY = "src/test/resources/conf/packed/config-recovery-fail-vertex-verify.properties";
    private final String FAIL_SUPERNODE = "src/test/resources/conf/packed/config-recovery-fail-supernodes.properties";

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
        System.out.println("Testing testSupernodeDetectionFailure");
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_SUPERNODE}, DEFAULT_PARAMS));
            Assert.fail("Should have thrown an exception");
        } catch (Exception ignored) {
            // Expected
        }
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME}, DEFAULT_PARAMS));
        Assert.assertEquals(vertexLineCount, graph.traversal().V().count().next().longValue());
        Assert.assertEquals(edgeLineCount, graph.traversal().E().count().next().longValue());
    }

    @Test
    public void testVertexWritingFailure() {
        System.out.println("Testing testVertexWritingFailure");
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_VERTEX_WRITE}, DEFAULT_PARAMS));
            Assert.fail("Should have thrown an exception");
        } catch (Exception ignored) {
            // Expected
        }
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME}, DEFAULT_PARAMS));
        Assert.assertEquals(vertexLineCount, graph.traversal().V().count().next().longValue());
        Assert.assertEquals(edgeLineCount, graph.traversal().E().count().next().longValue());
    }

    @Test
    public void testVertexVerificationFailure() {
        System.out.println("Testing testVertexVerificationFailure");
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_VERTEX_VERIFY}, DEFAULT_PARAMS));
            Assert.fail("Should have thrown an exception");
        } catch (Exception ignored) {
            // Expected
        }
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME}, DEFAULT_PARAMS));
        Assert.assertEquals(vertexLineCount, graph.traversal().V().count().next().longValue());
        Assert.assertEquals(edgeLineCount, graph.traversal().E().count().next().longValue());
    }

    @Test
    public void testEdgeWritingFailure() {
        System.out.println("Testing testEdgeWritingFailure");
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_EDGE_WRITE}, DEFAULT_PARAMS));
            Assert.fail("Should have thrown an exception");
        } catch (Exception ignored) {
            // Expected
        }
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME}, DEFAULT_PARAMS));
        Assert.assertEquals(vertexLineCount, graph.traversal().V().count().next().longValue());
        Assert.assertEquals(edgeLineCount, graph.traversal().E().count().next().longValue());
    }

    @Test
    public void testEdgeVerificationFailure() {
        System.out.println("Testing testEdgeVerificationFailure");
        try {
            SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_EDGE_VERIFY}, DEFAULT_PARAMS));
            Assert.fail("Should have thrown an exception");
        } catch (Exception ignored) {
            // Expected
        }
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME}, DEFAULT_PARAMS));
        Assert.assertEquals(vertexLineCount, graph.traversal().V().count().next().longValue());
        Assert.assertEquals(edgeLineCount, graph.traversal().E().count().next().longValue());
    }
}
