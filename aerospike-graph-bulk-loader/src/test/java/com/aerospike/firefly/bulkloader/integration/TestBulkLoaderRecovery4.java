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
public class TestBulkLoaderRecovery4 extends TestBulkLoaderRecovery {

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

    @Test
    public void testResumeWithoutIncremental() {
        graph.traversal().addV().next();
        graph.fireflySummaryUpdater.forceWrite();
        System.out.println("Testing testResume");
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", FAIL_SUPERNODE, "-" + INCREMENTAL_LOAD}, DEFAULT_PARAMS));
        waitForBulkLoadFail(graph.traversal());

        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig(), "-" + INCREMENTAL_LOAD}, DEFAULT_PARAMS));
        final String e = waitForBulkLoadFail(graph.traversal());
        System.out.println(e);
        Assert.assertTrue(e.contains("Cannot resume load without '" + RESUME + "' flag"));
        SparkBulkLoader.main(ArrayUtils.addAll(new String[]{"-local", "-c", getDefaultConfig(), "-" + FORCE, "-" + INCREMENTAL_LOAD}, DEFAULT_PARAMS));
        waitForBulkLoad(graph.traversal());
    }
}
