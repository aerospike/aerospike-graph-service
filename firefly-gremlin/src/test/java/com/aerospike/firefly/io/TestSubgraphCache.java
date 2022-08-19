package com.aerospike.firefly.io;

import com.aerospike.firefly.io.impl.SubgraphCache;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.IOUtil;
import com.aerospike.firefly.util.Util;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import static com.aerospike.firefly.Tokens.AIR_ROUTES_50K_URL;
import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphml;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestSubgraphCache {
    protected static final Configuration config;
    protected Logger LOG;
    protected static AerospikeConnection db;
    protected static FireflyGraph graph;

    static {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    }

    @BeforeClass
    public static void openGraph() throws IOException {
        db = AerospikeConnection.connect(config);
        graph = FireflyGraph.open(config);
        loadAirRoutes();
    }


    @AfterClass
    public static void closeGraphClearData() {
        Util.clearGraph(graph);
        graph.close();
        db.close();
    }

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


    public static void loadAirRoutes() throws IOException {
        if (!tempFile.exists()) IOUtil.downloadFileFromURL(airRoutesUrl, tempFile);
        g = graph.traversal();
        g.V().drop().iterate();
        graph.io(graphml()).readGraph(tempFile.getAbsolutePath());
    }


    @Test
    public void twoHopTest() throws IOException {
        Vertex aus = g.V().has("code", "AUS").next(); //need to get a specific starting point
        GraphTraversal<Vertex, Long> traversal = g.V(aus).out().out().dedup().count();
        ((Traversal.Admin)traversal).applyStrategies();
        System.out.println(traversal.toString());
        traversal.next();
        List<Vertex> airports = g.V().has("code").sample(3).toList();
    }

    @Test
    public void testDoesNotTriggerOnScans() {
//        long startHitCount = SubgraphCache.getHitCount();
        List<Vertex> res = g.V().has("code", "AUS").out().out().dedup().toList();
//        long secondHitCount = SubgraphCache.getHitCount();
//        assertEquals(startHitCount, secondHitCount);
    }

    @Test
    public void testDoesTriggerOnTwoOut() {
//        long startHitCount = SubgraphCache.getHitCount();
        List<Vertex> res = g.V(1).out().out().dedup().toList();
//        long secondHitCount = SubgraphCache.getHitCount();
//        assertTrue(secondHitCount > startHitCount);
    }

    @Test
    public void basic() {
//        long startHitCount = SubgraphCache.getHitCount();
        List<Vertex> res = g.V(1).out().out().dedup().toList();
//        long secondHitCount = SubgraphCache.getHitCount();
//        assertTrue(secondHitCount > startHitCount);
    }
}
