package com.aerospike.firefly.benchmark;

import com.aerospike.firefly.util.IOUtil;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;

import static com.aerospike.firefly.Tokens.AIR_ROUTES_50K_URL;

/**
 * Utility class to load the graph with specified dataset.
 */
public class GraphLoader {
    private static final Logger LOG = LoggerFactory.getLogger(GraphLoader.class);

    enum DATASET {
        FLIGHTS
    }

    public static void loadGraph(final GraphTraversalSource g, final DATASET dataset) {
        if (dataset == DATASET.FLIGHTS) {
            final File tempFile = new File(System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "air-routes50k.graphml");
            try {
                if (!tempFile.exists())
                    IOUtil.downloadFileFromURL(new URL(AIR_ROUTES_50K_URL), tempFile);
            } catch (Exception e) {
                LOG.error("Failed to download graphml file", e);
                throw new RuntimeException(e);
            }
            g.io(tempFile.getAbsolutePath()).with(IO.reader, IO.graphml).read().iterate();
        } else {
            throw new RuntimeException("Dataset not supported: " + dataset);
        }
    }
}
