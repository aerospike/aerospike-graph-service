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

import com.aerospike.firefly.runtime.FireflyServer;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Random;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class TestSupernodesTraverseredCounter {
    private static GraphTraversalSource g;
    private static DriverRemoteConnection connection;
    private static Vertex v1;
    private static FireflyServer server;

    @BeforeClass
    static public void beforeClass() {
        // Clear so there isn't any config issues.
        try (FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            graph.traversal().V().drop().iterate();
        }
    }

    @After
    public void afterEach() {
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
   public void fuzzSteps() {
       initializeServerWithAllSupernodes();
       for (int j = 0; j < 10; j++) {
           final Random rand = new Random();
           for (int k = 0; k < 3; k++) {
               GraphTraversal t = g.V(v1);
               for (int i = 0; i < 100; i++) {
                   switch (rand.nextInt(6)) {
                       case 0:
                           t = t.out();
                           if (rand.nextBoolean()) {
                               t = t.limit(1);
                           }
                           break;
                       case 1:
                           t = t.in();
                           if (rand.nextBoolean()) {
                               t = t.limit(1);
                           }
                           break;
                       case 2:
                           t = t.both();
                           if (rand.nextBoolean()) {
                               t = t.limit(1);
                           }
                           break;
                       case 3:
                           if (rand.nextBoolean()) {
                               t = t.outE().limit(1).has("dummy").otherV();
                           } else {
                               t = t.outE().has("dummy").otherV();
                           }
                           if (rand.nextBoolean()) {
                               t = t.limit(1);
                           }
                           break;
                       case 4:
                           if (rand.nextBoolean()) {
                               t = t.inE().limit(1).has("dummy").otherV();
                           } else {
                               t = t.inE().has("dummy").otherV();
                           }
                           if (rand.nextBoolean()) {
                               t = t.limit(1);
                           }
                           break;
                       case 5:
                           if (rand.nextBoolean()) {
                               t = t.bothE().limit(1).has("dummy").otherV();
                           } else {
                               t = t.bothE().has("dummy").otherV();
                           }
                           if (rand.nextBoolean()) {
                               t = t.limit(1);
                           }
                           break;
                   }
               }
               t.toList();
           }
           Assert.assertEquals(300L * (j + 1), SupernodesTraversedCounterUtil.getInstance().count());
       }
   }

   @Test
   public void testDisableWorks() {
       initializeServerWithCounterDisabled();
       g.V(v1).both().both().toList();
       Assert.assertEquals(0L, SupernodesTraversedCounterUtil.getInstance().count());
   }

   @Test
   public void testSomeSupernodes() {
       initializeServerWithSomeSupernodes();
       g.V(v1).out("1").out("1").toList();
       // Only 1 supernode, should be counted once.
       Assert.assertEquals(1L, SupernodesTraversedCounterUtil.getInstance().count());
   }

    public void initializeServerWithAllSupernodes() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, String.valueOf(0));
        server = FireflyServer.start(new String[] {"../conf/supernodes-traversed-count/firefly-gremlin-server-all-supernodes.yaml"});

        connection = DriverRemoteConnection.using("localhost", 8182);
        g = traversal().withRemote(connection);
        v1 = g.addV().next();
        g.addE("1").from(v1).to(v1).property("dummy", true).iterate();
    }

    public void initializeServerWithSomeSupernodes() {
        server = FireflyServer.start(new String[] {"../conf/supernodes-traversed-count/firefly-gremlin-server-some-supernodes.yaml"});
        connection = DriverRemoteConnection.using("localhost", 8182);
        g = traversal().withRemote(connection);
        // create chain A->B->A->B ... where A is a supernode but B is not.
        v1 = g.addV().next();
        final Vertex v2 = g.addV().next();
        g.addE("1").from(v1).to(v2).property("dummy", true).iterate();
        g.addE("1").from(v2).to(v1).property("dummy", true).iterate();
        g.addE("2").from(v1).to(__.addV()).iterate();
        g.addE("2").from(v1).to(__.addV()).iterate();
        g.addE("2").from(v1).to(__.addV()).iterate();
        g.addE("2").from(v1).to(__.addV()).iterate();
    }

    public void initializeServerWithCounterDisabled() {
        server = FireflyServer.start(new String[] {"../conf/supernodes-traversed-count/firefly-gremlin-server-supernode-count-disabled.yaml"});
        connection = DriverRemoteConnection.using("localhost", 8182);
        g = traversal().withRemote(connection);
        v1 = g.addV().next();
        g.addE("1").from(v1).to(v1).property("dummy", true).iterate();
    }
}
