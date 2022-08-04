package com.aerospike.firefly.performance;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.process.TestAerospikeGraphIntegration;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.IOUtil;
import com.aerospike.firefly.util.Util;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static com.aerospike.firefly.Tokens.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestMovielens1M {

    Logger logger = LoggerFactory.getLogger(TestAerospikeGraphIntegration.class);

    private static final Configuration config;

    static {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    }

    private AerospikeConnection db;
    private FireflyGraph graph;
    private GraphTraversalSource g;
    private static final File tempFile;
    private static final URL movieLensUrl;

    static {
        try {
            movieLensUrl = new URL(MOVIELENS_1M_URL);
            tempFile = new File(System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "movielens.kryo");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Before
    public void fetchMovieLens1M() {
        if (!tempFile.exists()) IOUtil.downloadFileFromURL(movieLensUrl, tempFile);
    }

    @Before
    public void openGraph() {
        this.db = AerospikeConnection.connect(config);
        graph = FireflyGraph.open(config);
        g = graph.traversal();
    }
    @Before
    public void clearGraph() {
        try (final FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            Util.clearGraph(graph);
        }
    }
    @After
    public void closeGraph() {
        graph.close();
        db.close();
    }

//    @Test
    public void testLoadMovieLens1M() {
        long start = System.currentTimeMillis();
        graph.traversal().io(tempFile.getAbsolutePath()).read().iterate();
        long finish = System.currentTimeMillis();
        long delta = finish - start;
        System.out.println(String.format("%d milliseconds elapsed", delta));
    }

}
