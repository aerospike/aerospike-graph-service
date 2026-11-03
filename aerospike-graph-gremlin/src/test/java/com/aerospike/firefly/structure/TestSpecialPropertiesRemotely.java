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
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.aerospike.firefly.util.DateTimeUtil.getDate;
import static com.aerospike.firefly.util.DateTimeUtil.testDateTimePropertiesCases;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;

public class TestSpecialPropertiesRemotely {

    private static FireflyServer server;
    private static FireflyGraph serverGraph;
    private static Cluster cluster;
    private static GraphTraversalSource g;

    @BeforeClass
    static public void setup() throws NoSuchFieldException, IllegalAccessException {
        server = FireflyServer.start(new String[]{"../conf/firefly-gremlin-server-github-actions.yaml"});
        final GremlinServer gremlinServer = (GremlinServer) ReflectionHelper.getFieldValue(server, "gremlinServer");
        serverGraph = (FireflyGraph) gremlinServer.getServerGremlinExecutor().getGraphManager().getGraph("graph");
        cluster = Cluster.build().addContactPoint("localhost").port(8182).create();
        g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));
    }

    @AfterClass
    public static void teardown() {
        try {
            g.close();
        } catch (final Exception e) {
            System.out.println("Exception occurred in AfterClass of TestSpecialPropertiesRemotely: " + e.getMessage());
        } finally {
            // DriverRemoteConnection.using(cluster, ...) does not own the cluster, so g.close() leaves it running
            // and its driver retries the stopped server for the rest of the JVM's life.
            cluster.close();
            // The server's graph config creates vertex property and label indexes in the shared test namespace;
            // drop them so later test classes see a clean namespace.
            serverGraph.db.dropDatabase(serverGraph, true);
            server.stop().join();
        }
    }

    @Before
    public void beforeEach() {
        Vertex person = g.addV("person").next();
        Vertex car = g.addV("vehicle").next();
        g.addE("bought")
                .property("year", "2022")
                .property("month", "dec")
                .from(person).to(car).iterate();
        g.addE("owns")
                .property("year", "2023")
                .property("month", "jan")
                .from(person).to(car).iterate();
    }

    @After
    public void afterEach() throws Exception {
        g.V().drop().iterate();
    }

    @Test
    public void testDateTimePropertiesOnFireflyServer() {
        testDateTimePropertiesCases(g);
    }

    @Test
    public void testDateTimePropertiesWithGremlinLang() throws ExecutionException, InterruptedException {
        final Client client = cluster.connect();

        List<Result> results = client.submit("g.addV().property('date', datetime('2022-10-02'))").all().get();
        assertEquals(1, results.size());
        Date date = (Date) results.get(0).getVertex().property("date").value();
        final Date expected = getDate(2022, Calendar.OCTOBER, 2);
        assertEquals(expected, date);
    }
}
