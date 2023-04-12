package com.aerospike.firefly.util;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestUtil {

    @Test
    public void canCopyFileFromResources() {

        try {
            final String resourceName = "integration-test-settings.properties";
            final Path tempPath = Files.createTempDirectory("firefly-test").toAbsolutePath();
            tempPath.toFile().deleteOnExit();
            IOUtil.copyResourceToDirectory(resourceName, tempPath);
            assertTrue(Files.list(tempPath).anyMatch(it -> it.getFileName().toString().equals(resourceName)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    @Test
    public void canConfigureMultipuleGraphs() {
        Configuration config_one = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        Configuration config_two = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);

        config_two.setProperty(ConfigurationHelper.Keys.GRAPH_ID.toLowerCase(), "6");
        AerospikeConnection db_one = AerospikeConnection.connect(config_one);
        AerospikeConnection db_two = AerospikeConnection.connect(config_two);
        db_one.dropDatabase();
        db_two.dropDatabase();
        FireflyGraph graph_one = FireflyGraph.open(config_one);
        graph_one.traversal().V().drop().iterate();
        FireflyGraph graph_two = FireflyGraph.open(config_two);
        graph_two.traversal().V().drop().iterate();

        Vertex a = graph_one.traversal().addV().next();
        a.addEdge("a", graph_one.traversal().addV().next());
        a.addEdge("a", graph_one.traversal().addV().next());
        assertEquals(3, graph_one.traversal().V().count().next().longValue());
        assertEquals(2, graph_one.traversal().E().count().next().longValue());

        Vertex b = graph_two.traversal().addV().next();
        b.addEdge("a", graph_two.traversal().addV().next());
        b.addEdge("a", graph_two.traversal().addV().next());
        assertEquals(3, graph_two.traversal().V().count().next().longValue());
        assertEquals(2, graph_two.traversal().E().count().next().longValue());

        graph_one.traversal().V().drop().iterate();
        graph_two.traversal().V().drop().iterate();
        graph_one.close();
        graph_two.close();
        db_one.close();
        db_two.close();
    }



}
