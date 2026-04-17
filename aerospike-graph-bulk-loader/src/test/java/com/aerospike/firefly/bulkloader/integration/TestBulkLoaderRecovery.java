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

package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutionException;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.getConfig;

public class TestBulkLoaderRecovery {

    protected final String DEFAULT_CONFIG = "src/test/resources/conf/packed/config-recovery.properties";
    protected final String FAIL_EDGE_WRITE = "src/test/resources/conf/packed/config-recovery-fail-edge-write.properties";
    protected final String FAIL_EDGE_VERIFY = "src/test/resources/conf/packed/config-recovery-fail-edge-verify.properties";
    protected final String FAIL_VERTEX_WRITE = "src/test/resources/conf/packed/config-recovery-fail-vertex-write.properties";
    protected final String FAIL_VERTEX_VERIFY = "src/test/resources/conf/packed/config-recovery-fail-vertex-verify.properties";
    protected final String FAIL_SUPERNODE = "src/test/resources/conf/packed/config-recovery-fail-supernodes.properties";
    protected final String EDGE_CACHE_GENERATION = "src/test/resources/conf/packed/config-recovery-fail-edge-cache-generation.properties";

    protected Configuration getTestConfig() {
        return getConfig(Path.of(DEFAULT_CONFIG));
    }

    protected String getDefaultConfig() {
        return DEFAULT_CONFIG;
    }

    protected static final String[] DEFAULT_PARAMS= {"-validate_input_data", "-verify_output_data"};
    protected FireflyGraph graph = null;
    protected static long vertexLineCount;
    protected static long edgeLineCount;

    @Before
    public void beforeEach() {
        Configuration config = getTestConfig();
        graph = FireflyGraph.open(config);
        graph.traversal().V().drop().iterate();
        RecoveryUtil.truncate(graph);
    }

    @BeforeClass
    public static void generateData() throws IOException, InterruptedException, ExecutionException {
        final Process python = Runtime.getRuntime().exec("python3 src/test/resources/csv-generate.py");
        if (python.onExit().get().exitValue() != 0) {
            throw new RuntimeException("Failed to generate csv data to run tests.");
        }

        // Count lines.
        final BufferedReader vertexReader =
                new BufferedReader(new FileReader("src/test/resources/recoverydata/vertices/vertexList.csv"));
        final BufferedReader edgeReader =
                new BufferedReader(new FileReader("src/test/resources/recoverydata/edges/edgeList.csv"));

        vertexLineCount = 0;
        while (vertexReader.readLine() != null) vertexLineCount++;
        vertexReader.close();

        edgeLineCount = 0;
        while (edgeReader.readLine() != null) edgeLineCount++;
        edgeReader.close();

        // Remove header.
        vertexLineCount--;
        edgeLineCount--;
    }

    @AfterClass
    public static void clearData() throws IOException {
        Files.deleteIfExists(Path.of("src/test/resources/recoverydata/vertices/vertexList"));
        Files.deleteIfExists(Path.of("src/test/resources/recoverydata/edges/edgeList"));
    }

    @After
    public void afterEach() {
        graph.close();
    }
}
