package com.aerospike.firefly.runtime;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class PrometheusServerTest {

    @Test
    public void testMetrics() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final String output = queryPrometheus();

            // Check cluster_name. Note default is empty string.
            Assert.assertTrue(output.contains("aerospike_graph_service_cluster_name{cluster_name=\"test\",}"));

            // Check that things are renamed.
            Assert.assertTrue(output.contains("G1_Survivor_Space"));
            Assert.assertTrue(output.contains("mapped_non_volatile_memory"));
            Assert.assertTrue(output.contains("aerospike_graph_service_jvm_info"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testMetricsRenameDisabled() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty("aerospike.graph.prometheus.rename.enabled", false);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final String output = queryPrometheus();

            // Check cluster_name. Note default is empty string.
            Assert.assertTrue(output.contains("aerospike_graph_service_cluster_name{cluster_name=\"test\",}"));

            // Check that things are renamed.
            Assert.assertTrue(output.contains("G1 Survivor Space"));
            Assert.assertTrue(output.contains("mapped - 'non-volatile memory'"));
            Assert.assertTrue(output.contains("aerospike_graph_service_jvm_info"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String queryPrometheus() throws IOException {
        final URL url = new URL("http://localhost:9090/metrics");
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
