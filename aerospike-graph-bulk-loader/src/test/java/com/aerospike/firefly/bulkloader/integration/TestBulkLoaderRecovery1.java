package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.bulkloader.SparkBulkLoader;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import static com.aerospike.firefly.bulkloader.integration.util.BulkLoadTestUtil.waitForBulkLoad;
import static com.aerospike.firefly.bulkloader.integration.util.BulkLoadTestUtil.waitForBulkLoadFail;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.FORCE;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.INCREMENTAL_LOAD;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.RESUME;

// This is split into two files because otherwise CI/CD will take forever. This lets us parallelize the testing.
public class TestBulkLoaderRecovery1 extends TestBulkLoaderRecovery {

    @Test
    public void testEdgeWritingFailure() {
        System.out.println("Testing testEdgeWritingFailure");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_EDGE_WRITE}, DEFAULT_PARAMS));
        waitForBulkLoadFail(graph.traversal());
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME}, DEFAULT_PARAMS));
        waitForBulkLoad(graph.traversal());
        Assert.assertEquals(vertexLineCount, graph.traversal().V().count().next().longValue());
        Assert.assertEquals(edgeLineCount, graph.traversal().E().count().next().longValue());
    }

    @Test
    public void testEdgeVerificationFailure() {
        System.out.println("Testing testEdgeVerificationFailure");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_EDGE_VERIFY}, DEFAULT_PARAMS));
        waitForBulkLoadFail(graph.traversal());
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME}, DEFAULT_PARAMS));
        waitForBulkLoad(graph.traversal());
        Assert.assertEquals(vertexLineCount, graph.traversal().V().count().next().longValue());
        Assert.assertEquals(edgeLineCount, graph.traversal().E().count().next().longValue());
    }

    @Test
    public void testDefault() {
        System.out.println("Testing testDefault");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
        waitForBulkLoad(graph.traversal());
        Assert.assertEquals(vertexLineCount, graph.traversal().V().count().next().longValue());
        Assert.assertEquals(edgeLineCount, graph.traversal().E().count().next().longValue());
    }

    @Test
    public void testResume() {
        System.out.println("Testing testResume");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_EDGE_VERIFY}, DEFAULT_PARAMS));
        waitForBulkLoadFail(graph.traversal());

        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig()}, DEFAULT_PARAMS));
        String e = waitForBulkLoadFail(graph.traversal());
        Assert.assertTrue(e.contains("Cannot resume load without '" + RESUME + "' flag"));

        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig(), "-" + INCREMENTAL_LOAD, "-" + RESUME}, DEFAULT_PARAMS));
        e = waitForBulkLoadFail(graph.traversal());
        Assert.assertTrue(e.contains("To resume a non-incremental load, the '" + INCREMENTAL_LOAD + "' flag must not be set. " +
                "Use only the '" + RESUME + "' flag to resume a non-incremental load."));

        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig(), "-" + FORCE, "-" + INCREMENTAL_LOAD}, DEFAULT_PARAMS));
        waitForBulkLoad(graph.traversal());
    }
}
