/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.runtime;

import com.aerospike.firefly.runtime.metrics.ServerMetrics;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.transaction.FireflyTransactionOpProcessor;
import com.aerospike.firefly.util.RepoPaths;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.ReflectionHelper;
import com.aerospike.firefly.util.WarmupUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.groovy.engine.GremlinExecutor;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.OpProcessor;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.apache.tinkerpop.gremlin.server.op.OpLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.TRAVERSAL_NAME;

/**
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
    private static Object sparkSession;

    public FireflyServer(final String file) {
        confPath = file;
    }

    public static void setSpark(final Object sparkSession) {
        FireflyServer.sparkSession = sparkSession;
    }

    public static void main(final String[] args) {
        start(args);
    }

    public static FireflyServer start(final String[] args) {
        if (args.length != 1) {
            logger.error("FireflyServer failed to start, no configuration file provided.");
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
            if (settings.graphs != null) {
                settings.graphs.replaceAll((graphName, graphPath) -> RepoPaths.expand(graphPath));
            }

            gremlinServer = new GremlinServer(settings);
            serverStarted = CompletableFuture.allOf(gremlinServer.start());

            // need to add TraversalSource's to GraphManager
            final GraphManager graphManager = gremlinServer.getServerGremlinExecutor().getGraphManager();

            final Map<String, OpProcessor> processors = (Map<String, OpProcessor>) ReflectionHelper.getFieldValue(OpLoader.class, null, "processors");
            if (processors.containsKey("session")) {
                processors.put("session", new FireflyTransactionOpProcessor());
            } else {
                // This should only occur if Tinkerpop updates and the internals no longer sets or uses this key
                throw new RuntimeException("Unexpectedly found no default processor in OpLoader. Please contact support.");
            }

            final Set<String> graphs = graphManager.getGraphNames();
            for (final String graphName : graphs) {
                final FireflyGraph graph = (FireflyGraph) graphManager.getGraph(graphName);
                graph.setSparkSession(sparkSession);
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

                if (FireflyGraph.NEED_PREHEAT && ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.AUTO_PRE_HEAT, graph.configuration())) {
                    logger.info("Starting warmup...");
                    WarmupUtil.create(graph.configuration()).preheat(WarmupUtil.passes);
                    FireflyGraph.NEED_PREHEAT = false;
                    logger.info("Warmup is complete.");
                }
            }
            FireflyGraph.NEED_PREHEAT = false;

            final String healthCheckFilename = System.getenv().get(ConfigurationHelper.Keys.HEALTHCHECK_FILE);
            if (StringUtils.isNotBlank(healthCheckFilename)) {
                new File(healthCheckFilename).createNewFile();
            }

            // workaround to set TraversalSource's for script engines
            final GremlinExecutor gremlinExecutor = gremlinServer.getServerGremlinExecutor().getGremlinExecutor();
            ReflectionHelper.setFieldValue(gremlinExecutor, "globalBindings", graphManager.getAsBindings());

            // workaround to allow only GremlinLangScriptEngine
            final FireflyScriptEngineManager scriptEngineManager = new FireflyScriptEngineManager();
            ReflectionHelper.setFieldValue(gremlinExecutor, "gremlinScriptEngineManager", scriptEngineManager);

            serverMetrics = new ServerMetrics(gremlinServer);
            serverMetrics.start();

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("Closing FireflyServer.");
                if (serverMetrics != null) {
                    serverMetrics.shutDown();
                    serverMetrics = null;
                }
            }, "firefly-server-shutdown"));
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
