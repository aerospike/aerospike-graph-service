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

import static com.aerospike.firefly.Tokens.AIR_ROUTES_50K_URL;

/**
 * Utility class to load the graph with specified dataset.
 */
public class BenchmarkTestUtils {
    private static final String LOCALHOST = "127.0.0.1";
    private static final Logger LOG = LoggerFactory.getLogger(BenchmarkTestUtils.class);
    private static final String DEFAULT_DATASET_SIZE = "1g";
    private static final String DEFAULT_STORAGE_TYPE = "mmd";

    enum DATASET {
        FLIGHTS
    }

    public static String getHost() {
        final String host = System.getProperty("firefly.host");
        return host == null ? LOCALHOST : host;
    }

    public static String getDefaultDatasetSize() {
        final String datasetSize = System.getProperty("dataset.size");
        return datasetSize == null ? DEFAULT_DATASET_SIZE : datasetSize;
    }

    public static String getStorageType() {
        final String storageType = System.getProperty("storage.type");
        return storageType == null ? DEFAULT_STORAGE_TYPE : storageType;
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

        final String urlLocation;
        if (dataset == DATASET.FLIGHTS) {
            urlLocation = AIR_ROUTES_50K_URL;
        } else {
            throw new RuntimeException("Dataset not supported: " + dataset + ".");
        }

        if (!getFireflyLocal(getHost())) {
            throw new RuntimeException("Firefly is not running locally, so the " + dataset + " dataset cannot be loaded.");
        }

        File tempFile = new File(System.getProperty("java.io.tmpdir") + System.getProperty("file.separator") + "air-routes50k.graphml");
        try {
            if (!tempFile.exists())
                IOUtil.downloadFileFromURL(new URL(urlLocation), tempFile);
        } catch (Exception e) {
            LOG.error("Failed to download graphml file", e);
            throw new RuntimeException(e);
        }
        g.io(tempFile.getAbsolutePath()).with(IO.reader, IO.graphml).read().iterate();
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
