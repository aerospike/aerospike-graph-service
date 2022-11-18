package com.aerospike.firefly.io;

import com.aerospike.firefly.io.impl.relational.star.packed.StarPackedGraph;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.IOUtil;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import static com.aerospike.firefly.Tokens.AIR_ROUTES_50K_URL;
import static com.aerospike.firefly.util.Util.verifyClean;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphml;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestDataRemoval extends AbstractFireflySuite {

    @Override
    protected boolean runTest() {
        // Do not run if the StarPackedGraph is being used. Takes too long.
        return !graph.getDataModel().equals(StarPackedGraph.DATA_MODEL);
    }

    @Override
    protected boolean clearData() {
        return true;
    }

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

    @Test
    @Ignore
    public void smallDataRemoval(){
        FireflyGraph graph = FireflyGraph.open(config);
        GraphHelper.cloneElements(TinkerFactory.createGratefulDead(), graph);
        graph.traversal().V().drop().iterate();
        verifyClean(graph);
    }

    @Test
    @Ignore
    public void largerDataRemoval() throws IOException {
        FireflyGraph graph = FireflyGraph.open(config);
        if (!tempFile.exists()) IOUtil.downloadFileFromURL(airRoutesUrl, tempFile);
        graph.io(graphml()).readGraph(tempFile.getAbsolutePath());
        graph.traversal().V().drop().iterate();
        verifyClean(graph);
    }

}
