package com.aerospike.firefly.benchmark;

import com.aerospike.firefly.util.IOUtil;
import org.apache.tinkerpop.gremlin.process.traversal.IO;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.runner.options.ChainedOptionsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.net.URL;
import java.nio.file.Path;

import static com.aerospike.firefly.Tokens.AIR_ROUTES_50K_URL;

/**
 * Utility class to load the graph with specified dataset.
 */
public class BenchmarkTestUtils {
    private static final String LOCALHOST = "127.0.0.1";
    private static final String FLIGHTS_DATASET_DOCKER = "/opt/air-routes/air-routes-50k.graphml";
    private static final String internaldataset = "/opt/internal dataset/internal dataset.graphml";
    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkTestUtils.class);

    enum DATASET {
        FLIGHTS,
        internaldataset
    }

    public static String getHost() {
        final String host = System.getProperty("firefly.host");
        return host == null ? LOCALHOST : host;
    }

    public static Integer getThreads() {
        final String threads = System.getProperty("benchmark.threads");
        return threads == null ? 1 : Integer.parseInt(threads);
    }

    public static Mode getMode(final Logger log) {
        final String mode = System.getProperty("benchmark.mode");
        final Mode benchmarkMode;
        if (mode == null) {
            benchmarkMode = Mode.AverageTime;
        } else if ("all".equalsIgnoreCase(mode)) {
            benchmarkMode = Mode.All;
        } else if ("throughput".equalsIgnoreCase(mode)) {
            benchmarkMode = Mode.Throughput;
        } else if ("average".equalsIgnoreCase(mode)) {
            benchmarkMode = Mode.AverageTime;
        } else {
            throw new RuntimeException("Error, could not get mode from system property 'benchmark.mode'. " +
                    "Valid values are: 'all', 'throughput', 'average'. Value provided: '" + mode + "'.");
        }
        log.info("Running benchmark with mode '{}'.", benchmarkMode.longLabel());
        return benchmarkMode;
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

        String ioLocation = null;
        String urlLocation = null;
        File tempFile = null;
        if (dataset == DATASET.FLIGHTS) {
            ioLocation = FLIGHTS_DATASET_DOCKER;
            urlLocation = AIR_ROUTES_50K_URL;
        } else if (dataset == DATASET.internaldataset) {
            ioLocation = internaldataset;
            urlLocation = null;
            tempFile = Path.of("../data/internal dataset.graphml").toFile();
        } else {
            throw new RuntimeException("Dataset not supported: " + dataset + ".");
        }

        if (getRunningInDocker()) {
            if (ioLocation == null) {
                throw new RuntimeException("Failed to find location for " + dataset + " dataset in docker.");
            }
            g.io(internaldataset).with(IO.reader, IO.graphml).read().iterate();
        } else if (getFireflyLocal(getHost())) {
            if (urlLocation == null && tempFile == null) {
                throw new RuntimeException("Failed to get url or file location for " + dataset + " dataset.");
            }
            if (tempFile == null) {
                tempFile = new File(System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "air-routes50k.graphml");
                try {
                    if (!tempFile.exists())
                        IOUtil.downloadFileFromURL(new URL(AIR_ROUTES_50K_URL), tempFile);
                } catch (Exception e) {
                    LOG.error("Failed to download graphml file", e);
                    throw new RuntimeException(e);
                }
            }
            g.io(tempFile.getAbsolutePath()).with(IO.reader, IO.graphml).read().iterate();
        } else {
            throw new RuntimeException("Firefly is not running in docker and not running locally, so the " + dataset + " dataset cannot be loaded.");
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
