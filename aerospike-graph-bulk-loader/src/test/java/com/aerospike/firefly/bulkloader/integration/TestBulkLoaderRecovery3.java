package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.bulkloader.SparkBulkLoader;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;

import static com.aerospike.firefly.bulkloader.integration.util.BulkLoadTestUtil.waitForBulkLoad;
import static com.aerospike.firefly.bulkloader.integration.util.BulkLoadTestUtil.waitForBulkLoadFail;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.*;

// This is split into two files because otherwise CI/CD will take forever. This lets us parallelize the testing.
public class TestBulkLoaderRecovery3 extends TestBulkLoaderRecovery {

    @Test
    public void testEdgeWritingFailure() {
        graph.traversal().addV().next();
        graph.fireflySummaryUpdater.forceWrite();
        System.out.println("Testing testEdgeWritingFailure");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_EDGE_WRITE, "-" + INCREMENTAL_LOAD}, DEFAULT_PARAMS));
        waitForBulkLoadFail(graph.traversal());
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME, "-" + INCREMENTAL_LOAD}, DEFAULT_PARAMS));
        waitForBulkLoad(graph.traversal());
        Assert.assertEquals(vertexLineCount + 1, graph.traversal().V().count().next().longValue());
        Assert.assertEquals(edgeLineCount, graph.traversal().E().count().next().longValue());
    }

    @Test
    public void testEdgeVerificationFailure() {
        graph.traversal().addV().next();
        graph.fireflySummaryUpdater.forceWrite();
        System.out.println("Testing testEdgeVerificationFailure");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_EDGE_VERIFY, "-" + INCREMENTAL_LOAD}, DEFAULT_PARAMS));
        waitForBulkLoadFail(graph.traversal());
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig(), "-" + RESUME, "-" + INCREMENTAL_LOAD}, DEFAULT_PARAMS));
        waitForBulkLoad(graph.traversal());
        Assert.assertEquals(vertexLineCount + 1, graph.traversal().V().count().next().longValue());
        Assert.assertEquals(edgeLineCount, graph.traversal().E().count().next().longValue());
    }
}
