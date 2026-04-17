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

package com.aerospike.firefly.runtime;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

public class PrometheusExporterTest {
    private static final String HOST = "localhost";
    private static final int PORT = 8182;
    private static final int HTTP_PORT1 = 9098;
    private static final int HTTP_PORT2 = 9099;
    private static final int DOCKER_HTTP_PORT = 9090;

    @Test
    public void testGremlinServerPrometheusExporter() throws Exception {
        final Cluster.Builder BUILDER = Cluster.build().addContactPoint(HOST).port(PORT).enableSsl(false);
        final DriverRemoteConnection drc = DriverRemoteConnection.using(BUILDER.create(), "g");
        final GraphTraversalSource g = traversal().withRemote(drc);
        final int traversalCount = getCountOfTraversals(DOCKER_HTTP_PORT);
        for (int i = 0; i < 1000; i++) {
            g.V().count().next();
        }
        Assert.assertEquals(traversalCount + 1000, getCountOfTraversals(DOCKER_HTTP_PORT));
    }

    private int getCountOfTraversals(final int port) throws IOException {
        String output = queryPrometheus(port);
        Assert.assertTrue(output.contains("aerospike_graph_service_GremlinServer_op_traversal_count "));
        output = output.split("aerospike_graph_service_GremlinServer_op_traversal_count ")[1];
        output = output.split("# HELP")[0];
        output = output.split("\\.")[0];
        return Integer.parseInt(output);
    }

    private String queryPrometheus(final int port) throws IOException {
        final URL url = new URL("http://" + HOST + ":" + port + "/metrics");
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
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.http.port", HTTP_PORT1);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            Assert.assertTrue(queryPrometheus(HTTP_PORT1).contains("aerospike_graph_service_jvm_memory_pool_bytes_used"));
        }
    }

    @Test
    public void testUsagePrometheus() {
        // Basic unit test to check that the prometheus server spins up and we can GET data from it. Prometheus is
        // not simple to parse ,so we are only checking existence.
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.usage.update.interval", 500);
        config.setProperty("aerospike.graph.http.port", HTTP_PORT2);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            Thread.sleep(500);
            String prometheus = queryPrometheus(HTTP_PORT2);
            String[] lines = prometheus.split("#");
            List<String> usageLines = Arrays.stream(lines).
                    filter(l -> l.contains("usage")).
                    map(String::trim).
                    filter(l -> l.startsWith("TYPE")).
                    collect(Collectors.toList());
            Assert.assertEquals(1, usageLines.size());
            String line = usageLines.get(0);
            String[] pieces = line.split(" ");
            String value = pieces[pieces.length - 1];
            final Double usage1 = Double.parseDouble(value);

            Thread.sleep(5000);

            prometheus = queryPrometheus(HTTP_PORT2);
            lines = prometheus.split("#");
            usageLines = Arrays.stream(lines).
                    filter(l -> l.contains("usage")).
                    map(String::trim).
                    filter(l -> l.startsWith("TYPE")).
                    collect(Collectors.toList());
            Assert.assertEquals(1, usageLines.size());
            line = usageLines.get(0);
            pieces = line.split(" ");
            value = pieces[pieces.length - 1];
            final Double usage2 = Double.parseDouble(value);

            Assert.assertTrue(usage2 > usage1);
        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
