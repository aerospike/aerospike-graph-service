package com.aerospike.firefly.benchmark;

import com.aerospike.firefly.util.IOUtil;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;

import static com.aerospike.firefly.Tokens.AIR_ROUTES_50K_URL;

/**
 * Utility class to load the graph with specified dataset.
 */
public class BenchmarkTestUtils {
    private static final String LOCALHOST = "127.0.0.1";
    private static final String FLIGHTS_DATASET_DOCKER = "/opt/air-routes/air-routes-50k.graphml";
    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkTestUtils.class);

    enum DATASET {
        FLIGHTS
    }

    public static String getHost() {
        final String host = System.getProperty("firefly.host");
        return host == null ? LOCALHOST : host;
    }

    public static Integer getThreads() {
        final String threads = System.getProperty("benchmark.threads");
        return threads == null ? 1 : Integer.parseInt(threads);
    }

    private static boolean getFireflyLocal(final String host) {
        return host.equals(LOCALHOST);
    }

    private static boolean getRunningInDocker() {
        return System.getProperty("docker.benchmark") != null;
    }

    private static boolean skipLoad() {
        return System.getProperty("skip.load") != null;
    }

    public static void loadGraph(final GraphTraversalSource g, final DATASET dataset) {
        if (skipLoad()) {
            LOG.info("Skipping load of dataset");
            return;
        }

        LOG.info("Clearing the graph.");
        g.V().drop().iterate();

        LOG.info("Loading the graph with " + dataset);
        if (dataset == DATASET.FLIGHTS) {
            if (getRunningInDocker()) {
                g.io(FLIGHTS_DATASET_DOCKER).with(IO.reader, IO.graphml).read().iterate();
            } else if (getFireflyLocal(getHost())) {
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
                throw new RuntimeException("Firefly is not running in docker and not running locally, so the " + dataset + " dataset cannot be loaded.");
            }
        } else {
            throw new RuntimeException("Dataset not supported: " + dataset);
        }
    }

    public static void appendJmhOptionsBuilder(final ChainedOptionsBuilder optionsBuilder) {
        if (getRunningInDocker()) {
            optionsBuilder.jvmArgsAppend("-Ddocker.benchmark=1");
        }
        if (!getFireflyLocal(getHost())) {
            optionsBuilder.jvmArgsAppend("-Dfirefly.host=" + getHost());
        }
    }
}
