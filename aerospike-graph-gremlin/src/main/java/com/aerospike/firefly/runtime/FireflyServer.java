package com.aerospike.firefly.runtime;

import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.WarmupUtil;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
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
            Settings settings = Settings.read(confPath);
            gremlinServer = new GremlinServer(settings);
            serverStarted = CompletableFuture.allOf(gremlinServer.start());

            // need to add TraversalSource's to GraphManager
            final GraphManager graphManager = gremlinServer.getServerGremlinExecutor().getGraphManager();

            boolean isWarmedUp = false;
            final Set<String> graphs = graphManager.getGraphNames();
            for (final String graphName : graphs) {
                final Graph graph = graphManager.getGraph(graphName);
                String gts = graph.configuration().getString(TRAVERSAL_NAME);
                if (gts == null) {
                    // default gts for default graph
                    if (graphName.equals("graph"))
                        gts = "g";
                    else
                        gts = "g" + graphName;
                }
                graphManager.putTraversalSource(gts, graph.traversal());

                if (!isWarmedUp) {
                    final boolean needPreheat = ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.AUTO_PRE_HEAT, graph.configuration());
                    if (needPreheat) {
                        WarmupUtil.create(graph.configuration()).preheat(WarmupUtil.passes);
                        isWarmedUp = true;
                        logger.info("Warmup is complete.");
                    }
                }
            }

            // workaround to set TraversalSource's for script engines
            final GremlinExecutor gremlinExecutor = gremlinServer.getServerGremlinExecutor().getGremlinExecutor();
            final Field globalBindings = GremlinExecutor.class.getDeclaredField("globalBindings");
            globalBindings.setAccessible(true);
            globalBindings.set(gremlinExecutor, graphManager.getAsBindings());
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
        serverStopped = gremlinServer.stop();
        return serverStopped;
    }
}