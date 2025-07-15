package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.bulkloader.SparkBulkLoader;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import static com.aerospike.firefly.bulkloader.integration.util.BulkLoadTestUtil.waitForBulkLoad;
import static com.aerospike.firefly.bulkloader.integration.util.BulkLoadTestUtil.waitForBulkLoadFail;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.INCREMENTAL_LOAD;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.RESUME;

// This is split into two files because otherwise CI/CD will take forever. This lets us parallelize the testing.
public class TestBulkLoaderRecovery4 extends TestBulkLoaderRecovery {

    @Test
    public void testSupernodeDetectionFailure() {
        graph.traversal().addV().next();
        graph.fireflySummaryUpdater.forceWrite();
        System.out.println("Testing testSupernodeDetectionFailure");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_SUPERNODE, "-" + INCREMENTAL_LOAD}, DEFAULT_PARAMS));
        waitForBulkLoadFail(graph.traversal());
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME, "-" + INCREMENTAL_LOAD}, DEFAULT_PARAMS));
        waitForBulkLoad(graph.traversal());
        Assert.assertEquals(vertexLineCount + 1, graph.traversal().V().count().next().longValue());
        Assert.assertEquals(edgeLineCount, graph.traversal().E().count().next().longValue());
    }

    @Test
    public void testEdgeCacheGenerationFailure() {
        graph.traversal().addV().next();
        graph.fireflySummaryUpdater.forceWrite();
        System.out.println("Testing testEdgeCacheGenerationFailure");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", EDGE_CACHE_GENERATION, "-" + INCREMENTAL_LOAD}, DEFAULT_PARAMS));
        waitForBulkLoadFail(graph.traversal());
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME, "-" + INCREMENTAL_LOAD}, DEFAULT_PARAMS));
        waitForBulkLoad(graph.traversal());
        Assert.assertEquals(vertexLineCount + 1, graph.traversal().V().count().next().longValue());
        Assert.assertEquals(edgeLineCount, graph.traversal().E().count().next().longValue());
    }

    @Test
    public void testVertexWritingFailure() {
        graph.traversal().addV().next();
        graph.fireflySummaryUpdater.forceWrite();
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_VERTEX_WRITE, "-" + INCREMENTAL_LOAD}, DEFAULT_PARAMS));
        waitForBulkLoadFail(graph.traversal());
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME, "-" + INCREMENTAL_LOAD}, DEFAULT_PARAMS));
        waitForBulkLoad(graph.traversal());
        Assert.assertEquals(vertexLineCount + 1, graph.traversal().V().count().next().longValue());
        Assert.assertEquals(edgeLineCount, graph.traversal().E().count().next().longValue());
    }

    @Test
    public void testVertexVerificationFailure() {
        graph.traversal().addV().next();
        graph.fireflySummaryUpdater.forceWrite();
        System.out.println("Testing testVertexVerificationFailure");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_VERTEX_VERIFY, "-" + INCREMENTAL_LOAD}, DEFAULT_PARAMS));
        waitForBulkLoadFail(graph.traversal());
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME, "-" + INCREMENTAL_LOAD}, DEFAULT_PARAMS));
        waitForBulkLoad(graph.traversal());
        Assert.assertEquals(vertexLineCount + 1, graph.traversal().V().count().next().longValue());
        Assert.assertEquals(edgeLineCount, graph.traversal().E().count().next().longValue());
    }
}
