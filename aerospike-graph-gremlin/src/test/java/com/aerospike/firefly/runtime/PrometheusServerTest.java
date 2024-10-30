package com.aerospike.firefly.runtime;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.junit.Assert.assertTrue;

public class PrometheusServerTest {
    private static final int PORT = 9094;

    @Test
    public void testMetrics() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.http.port", PORT);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final String output = queryPrometheus(PORT);

            // Check cluster_name. Note default is empty string.
            assertTrue(output.contains("aerospike_graph_service_cluster_name{cluster_name=\"test\",}"));

            // Check that things are renamed.
            assertTrue(output.contains("G1_Survivor_Space"));
            assertTrue(output.contains("mapped_non_volatile_memory"));
            assertTrue(output.contains("aerospike_graph_service_jvm_info"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testMetricsRenameDisabled() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.http.port", PORT);
        config.setProperty("aerospike.graph.prometheus.rename.enabled", false);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final String output = queryPrometheus(PORT);

            // Check cluster_name. Note default is empty string.
            assertTrue(output.contains("aerospike_graph_service_cluster_name{cluster_name=\"test\",}"));

            // Check that things are renamed.
            assertTrue(output.contains("G1 Survivor Space"));
            assertTrue(output.contains("mapped - 'non-volatile memory'"));
            assertTrue(output.contains("aerospike_graph_service_jvm_info"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testQueueSizeMetric() {
        FireflyServer server = null;
        GraphTraversalSource g;
        try {
            server = FireflyServer.start(new String[]{"../conf/test-metrics/firefly-gremlin-server-metrics-test.yaml"});

            // metrics update each 200ms
            Thread.sleep(300);

            // initial should be 0
            String output = queryPrometheus(9089);
            assertTrue(output.contains("aerospike_graph_service_com_aerospike_firefly_runtime_FireflyServer_gremlin_queue_size 0.0"));

            // lets create simple loop for infinite traversal
            final Cluster cluster = Cluster.build("localhost").maxInProcessPerConnection(100).port(8181).create();
            g = traversal().withRemote(DriverRemoteConnection.using(cluster));
            g.V().drop().iterate();
            final Vertex v1 = g.addV("test").next();
            final Vertex v2 = g.addV("test").next();
            g.addE("test").from(v1).to(v2).iterate();
            g.addE("test").from(v2).to(v1).iterate();

            // let's try to submit N queries
            final int threadCount = 10;
            final Thread[] threads = new Thread[threadCount];
            final GraphTraversalSource finalG = g;
            for (int i = 0; i < threadCount; i++) {
                threads[i] = new Thread(() -> {
                    try {
                        final Vertex v = finalG.with("evaluationTimeout", 2000).V(v1)
                                .repeat(__.out()).until(__.hasLabel("not-test")).next();
                    } catch (Exception ignored) {
                    }
                });
                threads[i].start();
            }

            int tryCount = 0;
            boolean success;
            do {
                Thread.sleep(200);

                // some queries still waiting...
                output = queryPrometheus(9089);

                // contains metric, but queue is not empty. Usually queue_size is 8.0, but not always
                success = output.contains("aerospike_graph_service_com_aerospike_firefly_runtime_FireflyServer_gremlin_queue_size")
                        && !output.contains("aerospike_graph_service_com_aerospike_firefly_runtime_FireflyServer_gremlin_queue_size 0.0");
            } while (tryCount++ < 5 && !success);
            assertTrue(success);

            // no luck for threads...
            for (int i = 0; i < threadCount; i++) {
                threads[i].interrupt();
            }
            // give some time to die for all threads
            Thread.sleep(2500);

            // now should be 0 in queue again
            output = queryPrometheus(9089);
            assertTrue(output.contains("aerospike_graph_service_com_aerospike_firefly_runtime_FireflyServer_gremlin_queue_size 0.0"));
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(e);
        } finally {
            traversal().withRemote(DriverRemoteConnection.using(Cluster.build("localhost").port(8181).create()))
                    .V().drop().iterate();

            if (server != null) {
                server.stop().join();
            }
        }
    }

    private String queryPrometheus(final int port) throws IOException {
        final URL url = new URL("http://localhost:" + port + "/metrics");
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
}
