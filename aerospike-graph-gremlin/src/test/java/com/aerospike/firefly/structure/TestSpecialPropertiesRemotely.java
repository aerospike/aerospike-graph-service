package com.aerospike.firefly.structure;

import com.aerospike.firefly.runtime.FireflyServer;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.aerospike.firefly.util.DateTimeUtil.testDateTimePropertiesCases;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;

public class TestSpecialPropertiesRemotely {

    private static FireflyServer server;

    @BeforeClass
    static public void setup() throws NoSuchFieldException, IllegalAccessException {
        server = FireflyServer.start(new String[]{"../conf/firefly-gremlin-server-local.yaml"});
    }

    @AfterClass
    public static void teardown() {
        server.stop().join();
    }

    @Test
    public void testDateTimePropertiesOnFireflyServer() {
        final Cluster cluster = Cluster.build().addContactPoint("localhost").port(8182).create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));

        g.V().drop().iterate();
        assertEquals(0L, g.V().count().next().longValue());

        testDateTimePropertiesCases(g, true);
    }

    @Test
    public void testDateTimePropertiesWithGremlinLang() throws ExecutionException, InterruptedException {
        final Cluster cluster = Cluster.build().addContactPoint("localhost").port(8182).create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));
        final Client client = cluster.connect();

        g.V().drop().iterate();
        assertEquals(0L, g.V().count().next().longValue());

        List<Result> results = client.submit("g.addV().property('date', datetime('2022-10-02'))").all().get();
        assertEquals(1, results.size());
    }
}
