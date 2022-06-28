package com.aerospike.firefly.performance;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.process.TestAerospikeGraphIntegration;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.IOUtil;
import com.aerospike.firefly.util.PerfUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Date;
import java.util.List;

import static com.aerospike.firefly.Tokens.AIR_ROUTES_50K_URL;
import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.apache.tinkerpop.gremlin.process.traversal.Operator.sum;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.*;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphml;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestAirRoutes50k {

    Logger logger = LoggerFactory.getLogger(TestAerospikeGraphIntegration.class);

    private static final Configuration config;

    static {
        config = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
    }

    private AerospikeConnection db;
    private FireflyGraph graph;
    private GraphTraversalSource g;
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

    @Before
    public void fetchAirRoutes50k() {
        if (!tempFile.exists()) IOUtil.downloadFileFromURL(airRoutesUrl, tempFile);
    }

    @Before
    public void openGraph() {
        this.db = AerospikeConnection.connect(config);
        db.dropDatabase();
        graph = FireflyGraph.open(config);
        g = graph.traversal();
    }

    @After
    public void closeGraph() {
        db.dropDatabase();
        graph.close();
        db.close();
    }

    @Test
    public void testLoadAirRoutes50K() throws IOException {
        long start = System.currentTimeMillis();
        graph.io(graphml()).readGraph(tempFile.getAbsolutePath());
        long finish = System.currentTimeMillis();
        long delta = finish - start;
        System.out.println(String.format("%d milliseconds elapsed", delta));
    }

    @Test
    public void testAirRoutes50KQueryLatency1() throws IOException {
        graph.io(graphml()).readGraph(tempFile.getAbsolutePath());
        PerfUtil.Results results = PerfUtil.runTestBatch(200, () -> {
            List<List<Object>> data = g.withSack(0).
                    V().has("code", "SAF").
                    repeat(outE().sack(sum).by("dist").inV()).times(2).limit(10).
                    order().by(sack()).
                    local(union(path().by("code").by("dist"),
                            sack()).fold()).
                    local(unfold().unfold().fold()).toList();
        });
        System.out.println(results);
    }
}
