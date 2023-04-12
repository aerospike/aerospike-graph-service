package com.aerospike.firefly.performance;

import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.IOUtil;
import com.aerospike.firefly.util.PerfUtil;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.aerospike.firefly.Tokens.AIR_ROUTES_50K_URL;
import static org.apache.tinkerpop.gremlin.process.traversal.Operator.sum;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.path;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.sack;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.unfold;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.union;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphml;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestAirRoutes50k extends AbstractFireflySuite {
    private static GraphTraversalSource g;
    private static final File tempFile;
    private static final URL airRoutesUrl;

    static {
        try {
            airRoutesUrl = new URL(AIR_ROUTES_50K_URL);
            tempFile = new File(System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "air-routes50k.graphml");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected boolean runTest() {
        // Do not run if the StarPackedGraph is being used. Takes too long.
        return !graph.getDataModel().equals(StarPackedGraph.DATA_MODEL);
    }


    @BeforeClass
    public static void loadAirRoutes() throws IOException, InterruptedException {
        Assume.assumeTrue(!graph.getDataModel().equals(StarPackedGraph.DATA_MODEL));
        graph.getBaseGraph().dropDatabase();
        if (!tempFile.exists()) IOUtil.downloadFileFromURL(airRoutesUrl, tempFile);
        g = graph.traversal();
        g.V().drop().iterate();
        long start = System.currentTimeMillis();
        System.out.println("Loading Air Routes 50k");
        graph.io(graphml()).readGraph(tempFile.getAbsolutePath());
        long finish = System.currentTimeMillis();
        long delta = finish - start;
        System.out.printf("Air Routes 50k Load Test: %d milliseconds elapsed%n", delta);
    }

    @Test
    public void testAirRoutes50KQueryLatency1() throws IOException {
        PerfUtil.Results results = PerfUtil.runTestBatch(200, () -> {
            List<List<Object>> data = g.withSack(0).
                    V().has("code", "SAF").
                    repeat(outE().sack(sum).by("dist").inV()).times(2).limit(10).
                    order().by(sack()).
                    local(union(path().by("code").by("dist"),
                            sack()).fold()).
                    local(unfold().unfold().fold()).toList();
        });
        System.out.println("Air routes 50k Query Latency:");
        System.out.println(results);
        System.out.println(g.withSack(0).
                V().has("code", "SAF").
                repeat(outE().sack(sum).by("dist").inV()).times(2).limit(10).
                order().by(sack()).
                local(union(path().by("code").by("dist"),
                        sack()).fold()).
                local(unfold().unfold().fold()).profile().next());
    }

    /**
     * Call this function in a separate thread and monitor execution.
     */
    public void runTraversal() {
        // This traversal will take days to run if the barrier does not release early.
        g.V().has("code", "SFO").
                repeat(__.out("route").
                        simplePath().repeat(__.in("route")).
                        times(3)).
                times(2).path().by("code").limit(3).toList();
    }

    @Test
    public void testCollectingBarrierExits() throws InterruptedException {
        // Create thread to run traversal separately so it doesn't block test execution if it hangs.
        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(this::runTraversal);

        // Submit traversal and get boolean for whether it completed or not.
        executorService.shutdown();
        final boolean traversalCompleted = executorService.awaitTermination(30, TimeUnit.SECONDS);
        if (!traversalCompleted) {
            // Did not complete, force it to shut down manually.
            executorService.shutdownNow();
        }

        // Fail if it did not shut down.
        Assert.assertTrue(traversalCompleted);
    }

    @Override
    protected boolean clearData() {
        return false;
    }
}
