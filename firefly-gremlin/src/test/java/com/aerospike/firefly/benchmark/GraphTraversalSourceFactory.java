package com.aerospike.firefly.benchmark;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.IOUtil;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.io.IoCore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;

import static com.aerospike.firefly.Tokens.AIR_ROUTES_50K_URL;
import static com.aerospike.firefly.Tokens.BENCHMARK_FIREFLY_PROPERTIES;
import static com.aerospike.firefly.benchmark.GraphTraversalSourceFactory.DATASET.FLIGHTS;
import static com.aerospike.firefly.benchmark.GraphTraversalSourceFactory.GRAPH.JANUSGRAPH;
import static org.apache.tinkerpop.gremlin.structure.io.IoCore.graphml;

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
            g.io(tempFile.getAbsolutePath()).read().iterate();
        } else {
            throw new RuntimeException("Dataset not supported: " + dataset);
        }
    }

    // TODO: Will need to load via remote connection. i.e GraphTraversalSource.
    static class AirRoutes {
        public static void loadAirRoutes50k(final Graph graph) {
            try {
                // LOG.info("Dropping current database.");
                // graph.traversal().V().drop().iterate();
                // LOG.info("Loading air routes 50k.");
                // final URL airRoutesUrl = new URL(AIR_ROUTES_50K_URL);
                // final File tempFile = new File(System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "air-routes50k.graphml");
                // if (!tempFile.exists())
                //     IOUtil.downloadFileFromURL(airRoutesUrl, tempFile);
                // graph.io(graphml()).readGraph(tempFile.getAbsolutePath());
                LOG.info("Air routes loaded.");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }
}
