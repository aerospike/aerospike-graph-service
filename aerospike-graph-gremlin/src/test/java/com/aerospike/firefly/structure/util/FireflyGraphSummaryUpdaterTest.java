package com.aerospike.firefly.structure.util;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.OutputCapturer;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.TTL_ENABLED_FLAG;

// Can run these manually, but if you run in the suite and a single test leaves the graph open, it causes them to not function properly.
public class FireflyGraphSummaryUpdaterTest {

    @Ignore
    @Test
    public void testExitsList() {
        final FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES));
        final List<FireflyGraph> graphs = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            graphs.add(FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES)));
        }

        // Shouldn't exit for any of these, should exit when the Firefly that is in the abstract class exits.
        for (final FireflyGraph g : graphs) {
            g.close();
            Assert.assertFalse(g.fireflySummaryUpdater.exited());
        }

        graph.close();
        Assert.assertTrue(graph.fireflySummaryUpdater.exited());
    }

    @Ignore
    @Test
    public void testReopen() {
        final FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES));
        final FireflyGraph graph2 = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES));

        graph.close();
        Assert.assertFalse(graph.fireflySummaryUpdater.exited());
        graph2.close();
        Assert.assertTrue(graph2.fireflySummaryUpdater.exited());

        final FireflyGraph graph3 = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES));
        Assert.assertFalse(graph3.fireflySummaryUpdater.exited());
        graph3.close();
        Assert.assertTrue(graph3.fireflySummaryUpdater.exited());
    }

    @Test
    public void testTtlNotReported() throws Exception {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.addProperty(TTL_ENABLED_FLAG.toLowerCase(), "true");
        try (final OutputCapturer outputCapturer = new OutputCapturer();
             final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.getBaseGraph().dropDatabase(graph, false);
            final GraphTraversalSource g = graph.traversal();
            final Vertex v1 = g.addV("person").property("~ttl", 100000).property("name", "simon").property("age", 12).next();
            final Vertex v2 = g.addV("earthling").property("~ttl", 100000).property("name", "valentyn").property("status", "giga").next();
            g.addV("human").property("name", "lyndon").property("height", "tall").next();
            final Vertex v3 = g.V().has("name", "lyndon").property("~ttl", 100000).next();
            g.addE("knows").property("duration", "years").property("~ttl", 100000).from(v1).to(v3).next();
            g.addE("manages").property("company", "aerospike").from(v3).to(v2).next();
            g.E().hasLabel("manages").property("~ttl", 100000).next();
            Thread.sleep(62000);

            final String[] logMessages = outputCapturer.getLines();
            boolean vLabelFound = false;
            boolean vPropFound = false;
            boolean eLabelFound = false;
            boolean ePropFound = false;
            for (final String line : logMessages) {
                Assert.assertFalse(line.contains("~ttl"));
                if (line.contains("Vertex count by label") && !line.contains("{}")) {
                    Assert.assertTrue(line.contains("earthling"));
                    Assert.assertTrue(line.contains("person"));
                    Assert.assertTrue(line.contains("human"));
                    vLabelFound = true;
                }
                if (line.contains("Vertex properties by label") && !line.contains("{}")) {
                    Assert.assertTrue(line.contains("name, status"));
                    Assert.assertTrue(line.contains("name, age"));
                    Assert.assertTrue(line.contains("name, height"));
                    vPropFound = true;
                }
                if (line.contains("Edge count by label") && !line.contains("{}")) {
                    Assert.assertTrue(line.contains("manages"));
                    Assert.assertTrue(line.contains("knows"));
                    eLabelFound = true;
                }
                if (line.contains("Edge properties by label") && !line.contains("{}")) {
                    Assert.assertTrue(line.contains("company"));
                    Assert.assertTrue(line.contains("duration"));
                    ePropFound = true;
                }
            }
            Assert.assertTrue(vLabelFound);
            Assert.assertTrue(vPropFound);
            Assert.assertTrue(eLabelFound);
            Assert.assertTrue(ePropFound);

            graph.getBaseGraph().dropDatabase(graph, false);
        }
    }
}
