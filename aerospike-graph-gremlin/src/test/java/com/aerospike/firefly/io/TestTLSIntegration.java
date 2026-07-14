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

package com.aerospike.firefly.io;

import com.aerospike.firefly.benchmark.BenchmarkTestUtils;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TestTLSIntegration {
    static {
        // Netty 4.2 enables TLS hostname verification by default; 4.1 did not.
        System.setProperty("io.netty.handler.ssl.defaultEndpointVerificationAlgorithm", "NONE");
    }

    private static final String HOST = BenchmarkTestUtils.getHost();
    private static final int PORT = 8182;
    private static final Cluster.Builder BUILDER = Cluster.build()
            .addContactPoint(HOST)
            .port(PORT)
            .enableSsl(true)
            .trustStore("../.github/aerospike/tls/truststore.jks").trustStorePassword("abc123");

    @Test
    public void testConnection() throws Exception {
        final Cluster cluster = BUILDER.create();
        try (final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster))) {
            g.V().drop().iterate();
            g.addV("test-tls").iterate();
            Assert.assertEquals(1, (long) g.V().hasLabel("test-tls").count().next());
            g.V().drop().iterate();
        }
    }
}
