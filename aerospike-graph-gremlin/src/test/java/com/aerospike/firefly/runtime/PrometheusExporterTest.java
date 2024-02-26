package com.aerospike.firefly.runtime;

import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class PrometheusExporterTest {
    private static final String HOST = "localhost";
    private static final int PORT = 8182;

    @Test
    public void testGremlinServerPrometheusExporter() throws Exception {
        final Cluster.Builder BUILDER = Cluster.build().addContactPoint(HOST).port(PORT).enableSsl(false);
        final DriverRemoteConnection drc = DriverRemoteConnection.using(BUILDER.create(), "g");
        final GraphTraversalSource g = traversal().withRemote(drc);
        final int traversalCount = getCountOfTraversals();
        for (int i = 0; i < 1000; i++) {
            g.V().count().next();
        }
        Assert.assertEquals(traversalCount + 1000, getCountOfTraversals());
    }

    public int getCountOfTraversals() throws IOException {
        String output = queryPrometheus();
        Assert.assertTrue(output.contains("aerospike_graph_service_GremlinServer_op_traversal_count "));
        output = output.split("aerospike_graph_service_GremlinServer_op_traversal_count ")[1];
        output = output.split("# HELP")[0];
        output = output.split("\\.")[0];
        return Integer.parseInt(output);
    }

    private String queryPrometheus() throws IOException {
        final URL url = new URL("http://" + HOST + ":9090/metrics");
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
        return content.toString();
    }


    @Test
    public void testSimplePrometheusExporter() throws Exception {
        // Basic unit test to check that the prometheus server spins up and we can GET data from it. Prometheus is
        // not simple to parse ,so we are only checking existence.
        HttpServer.create(9090, "/metrics", "/healthcheck").start();
        Assert.assertTrue(queryPrometheus().contains("aerospike_graph_service_jvm_memory_pool_bytes_used"));
    }
}
