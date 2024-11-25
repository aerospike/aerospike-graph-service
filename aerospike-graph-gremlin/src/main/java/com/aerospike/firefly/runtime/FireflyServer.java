package com.aerospike.firefly.runtime;

import com.aerospike.firefly.runtime.metrics.ServerMetrics;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.ReflectionHelper;
import com.aerospike.firefly.util.WarmupUtil;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.aerospike.firefly.util.ConfigurationHelper.Keys.TRAVERSAL_NAME;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 *
 * This is just a boilerplate server that for the purposes of 1.0 is only in our testing. In the future
 * we can consider adding plugins here and bootstrapping the server with this instead of gremlin-server.sh.
 */
public class FireflyServer {
    private static final Logger logger = LoggerFactory.getLogger(FireflyServer.class);
    private GremlinServer gremlinServer = null;
    private final String confPath;
    private CompletableFuture<Void> serverStarted = null;
    private CompletableFuture<Void> serverStopped = null;
    private ServerMetrics serverMetrics;

    public FireflyServer(final String file) {
        confPath = file;
    }

    public static void main(final String[] args) {
        start(args);
    }

    public static FireflyServer start(final String[] args) {
        if (args.length != 1) {
            System.err.println("Usage: Server <conf file>");
            System.exit(1);
        }

        String file = args[0];
        FireflyServer fireflyServer = new FireflyServer(file);
        fireflyServer.start().exceptionally(t -> {
            logger.error("Firefly Server was unable to start and will now begin shutdown", t);
            fireflyServer.stop().join();
            return null;
        }).join();
        return fireflyServer;
    }

    public synchronized CompletableFuture<Void> start() {
        if (serverStarted != null) {
            return serverStarted;
        }
        serverStarted = new CompletableFuture<>();
        try {
            final Settings settings = Settings.read(confPath);

            gremlinServer = new GremlinServer(settings);
            serverStarted = CompletableFuture.allOf(gremlinServer.start());

            // need to add TraversalSource's to GraphManager
            final GraphManager graphManager = gremlinServer.getServerGremlinExecutor().getGraphManager();

            final Set<String> graphs = graphManager.getGraphNames();
            for (final String graphName : graphs) {
                final FireflyGraph graph = (FireflyGraph) graphManager.getGraph(graphName);
                String gts = graph.configuration().getString(TRAVERSAL_NAME);
                if (gts == null) {
                    // default gts for default graph
                    if (graphName.equals("graph"))
                        gts = "g";
                    else
                        gts = "g" + graphName;
                }
                graphManager.putTraversalSource(gts, graph.traversal());

                // let's graph know his config file path to use with bulk loader
                graph.setConfigFilePath(settings.graphs.get(graphName));

                if (FireflyGraph.NEED_PREHEAT) {
                    final boolean needPreheat = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.AUTO_PRE_HEAT, graph.configuration());
                    if (needPreheat) {
                        WarmupUtil.create(graph.configuration()).preheat(WarmupUtil.passes);
                        FireflyGraph.NEED_PREHEAT = false;
                        logger.info("Warmup is complete.");
                    }
                }
            }
            FireflyGraph.NEED_PREHEAT = false;

            // workaround to set TraversalSource's for script engines
            final GremlinExecutor gremlinExecutor = gremlinServer.getServerGremlinExecutor().getGremlinExecutor();
            ReflectionHelper.setFieldValue(gremlinExecutor, "globalBindings", graphManager.getAsBindings());

            serverMetrics = new ServerMetrics(gremlinServer);
            serverMetrics.start();
        } catch (Exception ex) {
            serverStarted.completeExceptionally(ex);
        }
        return serverStarted;
    }

    public synchronized CompletableFuture<Void> stop() {
        if (gremlinServer == null) {
            return CompletableFuture.completedFuture(null);
        }
        if (serverStopped != null) {
            return serverStopped;
        }
        if (serverMetrics != null) {
            serverMetrics.shutDown();
            serverMetrics = null;
        }

        serverStopped = gremlinServer.stop();
        return serverStopped;
    }
}
