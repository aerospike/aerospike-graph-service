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

package com.aerospike.firefly.util;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public abstract class AbstractFireflySuite {
    @Rule public TestName testName = new TestName();
    private Instant start = Instant.now();
    private boolean isTestStarted = true;
    protected static final Configuration config;
    protected static Logger LOG;
    protected static AerospikeConnection db;
    protected static FireflyGraph graph;
    protected static Long evaluationTimeout = 10000L;

    protected abstract boolean clearData();
    protected boolean runTest() {
        return true;
    }

    static {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");
    }

    @BeforeClass
    public static void openGraph() {
        LOG = LoggerFactory.getLogger(AbstractFireflySuite.class);
        db = AerospikeConnection.connect(config);
        db.clearNamespace();
        graph = FireflyGraph.open(config);
        graph.getBaseGraph().dropDatabase(graph, false);
    }

    @Before
    public void beforeTest() {
        // Test check to see if we should run this test.
        Assume.assumeTrue(runTest());
        LOG.warn("===> Running " + testName.getMethodName() + " <===");
        if (clearData()) {
            this.isTestStarted = false;
            Util.cleanAndVerifyGraph(graph);
        }
        this.isTestStarted = true;
        start = Instant.now();
        exited = false;
    }

    @After
    public void printTestTime() {
        if (isTestStarted) {
            LOG.warn("===> " + testName.getMethodName() + " - " + Duration.between(start, Instant.now()).toMillis() + " ms <===");
        } else {
            LOG.warn("===> " + testName.getMethodName() + " did not start <===");
        }
    }

    @AfterClass
    public static void closeGraphClearData() {
        Util.cleanAndVerifyGraph(graph);
        db.clearNamespace();
        graph.close();
    }

    public static boolean exited = false;
    public static class ExitManagerTest extends FireflyGraph.ExitManager {
        @Override
        public void exit(final int code) {
            exited = true;
        }
    }
}
