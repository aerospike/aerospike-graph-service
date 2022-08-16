package com.aerospike.firefly.io;

import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.IOUtil;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.List;

import static com.aerospike.firefly.Tokens.AIR_ROUTES_50K_URL;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphml;
import static org.junit.Assert.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestSubgraphCache extends AbstractFireflySuite {
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
        loadAirRoutes();
        long startHitCount = graph.getBaseGraph().subgraphCache.getHitCount();
        long startMissCount = graph.getBaseGraph().subgraphCache.getMissCount();
        Vertex aus = g.V().has("code", "AUS").next(); //need to get a specific starting point
        Long res = g.V(aus).out().out().dedup().count().next();
        List<Vertex> airports = g.V().has("code").sample(3).toList();
        long secondHitCount = graph.getBaseGraph().subgraphCache.getHitCount();
        long secondMissCount = graph.getBaseGraph().subgraphCache.getMissCount();
        assertTrue(secondHitCount > startHitCount);
    }
}
