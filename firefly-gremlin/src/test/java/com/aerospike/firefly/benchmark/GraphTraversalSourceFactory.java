package com.aerospike.firefly.benchmark;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.Tokens.BENCHMARK_FIREFLY_PROPERTIES;

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

    public static Graph createGraphTraversalSource(GRAPH graph) {
        switch (graph) {
            case FIREFLY:
                return createFireflyGraphTraversalSource();
            case JANUSGRAPH:
                return createJanusGraphTraversalSource();
            default:
                return createFireflyGraphTraversalSource();
        }
    }

    public static void loadGraph(GRAPH graph, DATASET dataset) {
        switch (graph) {
            case FIREFLY: {
                FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromResources(BENCHMARK_FIREFLY_PROPERTIES));
                AirRoutes.loadAirRoutes50k(fireflyGraph);
                fireflyGraph.close();
            }
            case JANUSGRAPH:
                AirRoutes.loadAirRoutes50k(null);
            default: {
                FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromResources(BENCHMARK_FIREFLY_PROPERTIES));
                AirRoutes.loadAirRoutes50k(fireflyGraph);
                fireflyGraph.close();
            }
        }
    }

    // TODO: Connect using remote connection and generate graph traversal source.
    //  Currently using Graph object so it can be closed after the test.
    private static Graph createFireflyGraphTraversalSource() {
        LOG.info("Creating Firefly GraphTraversalSource.");
        return FireflyGraph.open(ConfigurationHelper.loadFromResources(BENCHMARK_FIREFLY_PROPERTIES));
    }

    // TODO: Support JanusGraph
    private static Graph createJanusGraphTraversalSource() {
        throw new RuntimeException("JanusGraph is not yet supported.");
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
