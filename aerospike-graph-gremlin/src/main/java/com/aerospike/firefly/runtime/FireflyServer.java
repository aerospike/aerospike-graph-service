package com.aerospike.firefly.runtime;

import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.server.Settings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;

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