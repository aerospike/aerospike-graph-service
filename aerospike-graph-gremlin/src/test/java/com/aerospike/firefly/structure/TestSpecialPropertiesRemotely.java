package com.aerospike.firefly.structure;

import com.aerospike.firefly.runtime.FireflyServer;
import org.apache.tinkerpop.gremlin.driver.Client;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.Result;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.aerospike.firefly.util.DateTimeUtil.testDateTimePropertiesCases;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;

public class TestSpecialPropertiesRemotely {

    private static FireflyServer server;
    private static Cluster cluster;
    private static GraphTraversalSource g;

    @BeforeClass
    static public void setup() throws NoSuchFieldException, IllegalAccessException {
        server = FireflyServer.start(new String[]{"../conf/firefly-gremlin-server-local.yaml"});
        cluster = Cluster.build().addContactPoint("localhost").port(8182).create();
        g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));
    }

    @AfterClass
    public static void teardown() {
        server.stop().join();
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
    }
}
