package com.aerospike.firefly.benchmark;

import com.aerospike.firefly.util.IOUtil;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;

import static com.aerospike.firefly.Tokens.AIR_ROUTES_50K_URL;
import static com.aerospike.firefly.benchmark.GraphTraversalSourceFactory.DATASET.FLIGHTS;

// TODO This should not create the graph and then return a GraphTraversalSource.
//      Instead the GraphTraversalSource should be created as a RemoteConnection to
//      an instance of gremlin-server that is hosted separately.
public class GraphTraversalSourceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(GraphTraversalSourceFactory.class);

    enum GRAPH {
        FIREFLY,
        JANUSGRAPH
    }

    enum DATASET {
        FLIGHTS
    }

    public static void loadGraph(GraphTraversalSource g, DATASET dataset) {
        if (dataset == FLIGHTS) {
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
