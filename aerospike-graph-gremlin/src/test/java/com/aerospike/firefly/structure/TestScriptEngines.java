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

package com.aerospike.firefly.structure;

import com.aerospike.firefly.runtime.FireflyServer;
import com.aerospike.firefly.util.ReflectionHelper;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.RequestOptions;
import org.apache.tinkerpop.gremlin.driver.exception.ResponseException;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.util.message.ResponseStatusCode;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.concurrent.CompletionException;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class TestScriptEngines {

    private static FireflyServer server;

    @BeforeClass
    static public void setup() throws NoSuchFieldException, IllegalAccessException {
        server = FireflyServer.start(new String[]{"../conf/firefly-gremlin-server-multi-tenant.yaml"});

        // temporary solution to init modern graph
        final GremlinServer gremlinServer = (GremlinServer) ReflectionHelper.getFieldValue(server, "gremlinServer");
        final GraphManager graphManager = gremlinServer.getServerGremlinExecutor().getGraphManager();
        final Graph graph = graphManager.getGraph("modern");

        if (!graph.vertices().hasNext()) {
            GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        }
    }

    @AfterClass
    public static void teardown() {
        server.stop().join();
    }

    @Test
    public void gremlinLang() {
        final Cluster cluster = Cluster.build().addContactPoint("localhost").port(8182).create();
        final Client client = cluster.connect();

        // should handle traversal
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster, "gmodern"));
        long count = g.V().count().next();
        assertEquals(6, count);

        // should handle script
        final RequestOptions noLang = RequestOptions.build().addAlias("g", "gmodern").create();
        count = client.submit("g.V().count().next()", noLang).one().getLong();
        assertEquals(6, count);

        // should handle script with explicit engine name
        final RequestOptions requestOptions = RequestOptions.build().language("gremlin-lang").addAlias("g", "gmodern").create();
        count = client.submit("g.V().count().next()", requestOptions).one().getLong();
        assertEquals(6, count);

        // should not allow suspicious queries
        try {
            client.submit("2+2", requestOptions).one().getLong();
            fail("should throw exception");
        } catch (final CompletionException e) {
            final Throwable inner = e.getCause();
            assertTrue(inner instanceof ResponseException);
            assertEquals(ResponseStatusCode.SERVER_ERROR_EVALUATION, ((ResponseException) inner).getResponseStatusCode());
        }

        // in gremlin-groovy '1g' is valid BigDecimal value, but in gremlin-lang should be '1m';
        // the easiest way to determine which script engine the request was executed on
        try {
            client.submit("g.inject(1g)", requestOptions).one().getLong();
            fail("should throw exception");
        } catch (final CompletionException e) {
            final Throwable inner = e.getCause();
            assertTrue(inner instanceof ResponseException);
            assertEquals(ResponseStatusCode.SERVER_ERROR_EVALUATION, ((ResponseException) inner).getResponseStatusCode());
        }

        final BigDecimal one = (BigDecimal)client.submit("g.inject(1m)", requestOptions).one().getObject();
        assertEquals(BigDecimal.ONE, one);
    }

    public void demo() {
        final Cluster cluster = Cluster.build().addContactPoint("localhost").port(8182).create();
        final Client client = cluster.connect();

        final long sum = client.submit("System.out.println(\"hello\");2+2").one().getLong();
        assertEquals(4, sum);

        try {
            final RequestOptions requestOptions = RequestOptions.build().language("gremlin-lang").create();
            client.submit("System.out.println(\"hello\");2+2", requestOptions).one().getLong();
            fail("should throw exception");
        } catch (final CompletionException e) {
            final Throwable inner = e.getCause();
            assertTrue(inner instanceof ResponseException);
            assertEquals(ResponseStatusCode.SERVER_ERROR_EVALUATION, ((ResponseException) inner).getResponseStatusCode());
        }

        final RequestOptions requestOptions = RequestOptions.build().language("gremlin-lang").addAlias("g", "gmodern").create();
        final long count = client.submit("g.V().count().next()", requestOptions).one().getLong();
        assertEquals(6, count);
    }
}
