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

package com.aerospike.firefly.tx;

import com.aerospike.firefly.runtime.FireflyServer;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.TRANSACTION_TIMEOUT;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TestTinkerpopTransactionTimeout {
    private static GraphTraversalSource g;
    private static DriverRemoteConnection connection;
    private static FireflyServer server;

    @BeforeClass
    public static void beforeClass() {
        // Clear so there isn't any config issues.
        try (FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            graph.traversal().V().drop().iterate();
        }
        server = FireflyServer.start(new String[] {"../conf/transaction-timeout/firefly-gremlin-server-txn-timeout.yaml"});
        try {
            // Wait for server to start so log isn't spammed with reconnection attempts.
            Thread.sleep(1000);
        } catch (InterruptedException e) {
        }
        connection = DriverRemoteConnection.using("localhost", 8182);
        g = traversal().withRemote(connection);
    }

    @AfterClass
    public static void afterAll() {
        if (connection != null) {
            try {
                connection.close();
            } catch (final Exception ignored) {}
        }

        if (server != null) {
            try {
                server.stop().join();
            } catch (final Exception ignored) {}
        }

        // Clear again b/c of configs.
        try (final FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            graph.traversal().V().drop().iterate();
        }
    }

    @Test
    public void testDefaultTimeout() {
        final GraphTraversalSource gtx = g.tx().begin();
        gtx.addV().next();
        final Instant instant = Instant.now();
        final int defaultTimeout = 10;
        for (int i = 0; i < 1500; i++) {
            try {
                gtx.addV().next();
            } catch (final Exception e) {
                // Expected
                Assert.assertTrue(e.getMessage().contains("Transaction has timed out. Please refer to documentation on " +
                        "configuring transaction timeouts or contact support if the problem persists."));
                break;
            }
            try {
                Thread.sleep(10);
            } catch (final InterruptedException ignored) {
            }
            if (Duration.between(instant, Instant.now()).toMillis() > (defaultTimeout * 1000L + 1000)) {
                Assert.fail("Expected to timeout before this long");
            }
        }
    }

    @Test
    public void testSettingTimeoutManyAddV() {
        final int smallTimeout = 1000;
        final GraphTraversalSource gtx = (GraphTraversalSource) g.tx().begin().with(TRANSACTION_TIMEOUT, smallTimeout / 1000);
        gtx.addV().next();
        final Instant instant = Instant.now();
        for (int i = 0; i < 1500; i++) {
            try {
                gtx.addV().next();
            } catch (final Exception e) {
                // Expected
                Assert.assertTrue(e.getMessage().contains("Transaction has timed out. Please refer to documentation on " +
                        "configuring transaction timeouts or contact support if the problem persists."));
                break;
            }
            try {
                Thread.sleep(1);
            } catch (final InterruptedException ignored) {
            }
            if (Duration.between(instant, Instant.now()).toMillis() > (smallTimeout + 1000)) {
                Assert.fail("Expected to timeout before this long");
            }
        }
    }

    @Test
    public void testSettingTimeoutSimpleAddV() {
        final int smallTimeout = 1000;
        final GraphTraversalSource gtx = (GraphTraversalSource) g.tx().begin().with(TRANSACTION_TIMEOUT, smallTimeout / 1000);
        gtx.addV().next();
        try {
            Thread.sleep(smallTimeout + 1000);
        } catch (final InterruptedException ignored) {
        }
        try {
            gtx.addV().next();
            gtx.tx().commit();
            Assert.fail("Should have timed out");
        } catch (final Exception e) {
            // Expected
            Assert.assertTrue(e.getMessage().contains("Transaction has timed out. Please refer to documentation on " +
                    "configuring transaction timeouts or contact support if the problem persists."));
        }
    }
}
