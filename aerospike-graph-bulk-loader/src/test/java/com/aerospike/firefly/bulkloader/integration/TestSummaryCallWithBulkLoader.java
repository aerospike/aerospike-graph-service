package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.aerospike.firefly.bulkloader.integration.util.BulkLoadTestUtil.waitForBulkLoad;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.INCREMENTAL_LOAD;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.getConfig;

public class TestSummaryCallWithBulkLoader {

    private final String DEFAULT_CONFIG = "src/test/resources/conf/packed/config.properties";

    private Configuration getTestConfig() {
        return getConfig(Path.of(DEFAULT_CONFIG));
    }

    protected FireflyGraph graph = null;

    @BeforeClass
    public static void generateData() throws IOException, InterruptedException, ExecutionException {
        final Process python = Runtime.getRuntime().exec(
                "python3 src/test/resources/csv-generate-summary-count.py");
        if (python.onExit().get().exitValue() != 0) {
            throw new RuntimeException("Failed to generate csv data to run tests.");
        }
    }

    @Before
    public void beforeEach() {
        Configuration config = getTestConfig();
        graph = FireflyGraph.open(config);
        // Set config for the bulk loader
        graph.setConfigFilePath(DEFAULT_CONFIG);
        graph.traversal().V().drop().iterate();
        RecoveryUtil.truncate(graph);
    }

    @Test
    public void testStandaloneBulkLoaderSupernodeCount() {
        final GraphTraversalSource g = graph.traversal();

        // Test empty summary
        final Map<Object, Object> summaryCallEmpty = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();
        final Map<Object, Object> expectedEmpty = Map.of(
                "Vertex count by label", Map.of(),
                "Edge count by label", Map.of(),
                "Supernode count by label", Map.of(),
                "Edge properties by label", Map.of(),
                "Vertex properties by label", Map.of(),
                "Total vertex count", 0L,
                "Total edge count", 0L,
                "Total supernode count", 0L);
        Assert.assertEquals(expectedEmpty, summaryCallEmpty);

        // Initial Load
        g.with("evaluationTimeout", 20000).
                call("aerospike.graphloader.admin.bulk-load.load").
                with("aerospike.graphloader.vertices", "src/test/resources/sampledata-incremental-supernode-count/vertices-init").
                with("aerospike.graphloader.edges", "src/test/resources/sampledata-incremental-supernode-count/edges-init")
                .with(INCREMENTAL_LOAD, false).next();

        waitForBulkLoad(g);

        final long vertexCount = g.V().count().next();
        final long edgeCount = g.E().count().next();
        final long supernodeCount = g.V()
                .filter(__.bothE().count().is(P.gt(graph.getBaseGraph().ON_RECORD_ID_LIMIT)))
                .count()
                .next();

        final Map<Object, Object> vertexLabels = g.V().group().by(__.label()).by(__.count()).next();
        final Map<Object, Object> edgeLabels = g.E().group().by(__.label()).by(__.count()).next();
        final Map<Object, Object> supernodeLabels = g.V()
                .filter(__.bothE().count().is(P.gt(graph.getBaseGraph().ON_RECORD_ID_LIMIT)))
                .group()
                .by(__.label())
                .by(__.count())
                .next();

        Map<Object, Object> summaryCallGrateful = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();
        Assert.assertEquals(vertexLabels, summaryCallGrateful.get("Vertex count by label"));
        Assert.assertEquals(edgeLabels, summaryCallGrateful.get("Edge count by label"));
        Assert.assertEquals(supernodeLabels, summaryCallGrateful.get("Supernode count by label"));
        Assert.assertEquals(6L, vertexCount);
        Assert.assertEquals(vertexCount, summaryCallGrateful.get("Total vertex count"));
        Assert.assertEquals(20000L, edgeCount);
        Assert.assertEquals(edgeCount, summaryCallGrateful.get("Total edge count"));
        Assert.assertEquals(3L, supernodeCount);
        Assert.assertEquals(supernodeCount, summaryCallGrateful.get("Total supernode count"));

        // Incremental Load
        g.with("evaluationTimeout", 20000).
                call("aerospike.graphloader.admin.bulk-load.load").
                with("aerospike.graphloader.vertices", "src/test/resources/sampledata-incremental-supernode-count/vertices-incremental").
                with("aerospike.graphloader.edges", "src/test/resources/sampledata-incremental-supernode-count/edges-incremental").
                with(INCREMENTAL_LOAD, true).next();

        waitForBulkLoad(g);

        summaryCallGrateful = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();
        Assert.assertEquals(5L, ((Map<Object, Object>) summaryCallGrateful.get("Vertex count by label")).get("movie"));
        Assert.assertEquals(3L, ((Map<Object, Object>) summaryCallGrateful.get("Vertex count by label")).get("person"));
        Assert.assertEquals(49000L, ((Map<Object, Object>) summaryCallGrateful.get("Edge count by label")).get("knows"));
        Assert.assertEquals(3L, ((Map<Object, Object>) summaryCallGrateful.get("Supernode count by label")).get("movie"));
        Assert.assertEquals(3L, ((Map<Object, Object>) summaryCallGrateful.get("Supernode count by label")).get("person"));
        Assert.assertEquals(8L, summaryCallGrateful.get("Total vertex count"));
        Assert.assertEquals(49000L, summaryCallGrateful.get("Total edge count"));
        // Supernode increased by 3:
        // 2 new vertices which are supernodes and 1 existing vertex promoted to be a supernode.
        Assert.assertEquals(6L, summaryCallGrateful.get("Total supernode count"));
    }
}
