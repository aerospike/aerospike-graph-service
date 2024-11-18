package com.aerospike.firefly.structure;

import com.aerospike.firefly.runtime.FireflyServer;
import com.aerospike.firefly.util.ReflectionHelper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectReader;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.server.GraphManager;
import org.apache.tinkerpop.gremlin.server.GremlinServer;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class TestMultiTenant {

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
    public void testDifferentGraphsOnSameServer() throws Exception {
        final Cluster cluster = Cluster.build().addContactPoint("localhost").port(8182).create();

        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));
        final GraphTraversalSource gModern = traversal().withRemote(DriverRemoteConnection.using(cluster, "gmodern"));

        g.V().drop().iterate();
        assertEquals(0L, g.V().count().next().longValue());
        assertEquals(6L, gModern.V().count().next().longValue());

        g.addV("test").iterate();
        assertEquals(1L, g.V().count().next().longValue());
        assertEquals(6L, gModern.V().count().next().longValue());

        g.V().drop().iterate();
        assertEquals(0L, g.V().count().next().longValue());
        assertEquals(6L, gModern.V().count().next().longValue());
    }

    @Test
    public void testAdminAPI() throws Exception {
        final Cluster cluster = Cluster.build().addContactPoint("localhost").port(8182).create();
        final GraphTraversalSource g = traversal().withRemote(DriverRemoteConnection.using(cluster, "g"));

        g.V().drop().iterate();
        waitForExpectedResult("0/admin/metadata/summary", 0);
        waitForExpectedResult("modern/admin/metadata/summary", 6);

        g.addV("test").iterate();
        Thread.sleep(100);
        waitForExpectedResult("0/admin/metadata/summary", 1);
        waitForExpectedResult("modern/admin/metadata/summary", 6);

        g.V().drop().iterate();
    }

    private void waitForExpectedResult(final String path, final int expected) throws IOException, InterruptedException {
        int iterations = 0;
        while (iterations++ < 20) {
            final JsonNode summary = queryAdminAPI(path);
            if (expected == summary.get("Total vertex count").asInt()) {
                return;
            }
            System.out.println("try " + iterations + ", " + summary.get("Total vertex count").asInt() + " != " + expected);
            // usually need 150-200ms locally, so let's try several times with 50ms delay
            Thread.sleep(50);
        }
        fail("Admin API didn't return expected result");
    }

    private JsonNode queryAdminAPI(final String path) throws IOException {
        final URL url = new URL("http://localhost:9090/" + path);
        final HttpURLConnection con = (HttpURLConnection) url.openConnection();
        con.setRequestMethod("GET");
        final int response = con.getResponseCode();
        final BufferedReader reader = new BufferedReader(new InputStreamReader(con.getInputStream()));
        final StringBuffer content = new StringBuffer();
        String inputLine;
        while ((inputLine = reader.readLine()) != null) {
            content.append(inputLine);
        }
        reader.close();
        con.disconnect();

        final ObjectReader mapper = new ObjectMapper().reader();
        return mapper.readTree(content.toString());
    }
}
