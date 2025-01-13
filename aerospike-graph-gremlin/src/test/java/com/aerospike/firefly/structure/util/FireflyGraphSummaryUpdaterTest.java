package com.aerospike.firefly.structure.util;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

// Can run these manually, but if you run in the suite and a single test leaves the graph open, it causes them to not function properly.
@Ignore
public class FireflyGraphSummaryUpdaterTest {

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
}
