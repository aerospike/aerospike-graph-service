package com.aerospike.firefly.call;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestFireflyBasicCall {

    @Test
    public void testBasicCall() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            final List<Object> normalOutput = g.call("--list").toList();
            final List<Object> verboseOutput = g.call("--list").with("verbose").toList();

            // The output is a list of strings that looks like:
            // [summary, bulk-load]
            Assert.assertEquals(new HashSet<>(normalOutput), Set.of("summary", "bulk-load", "usage-stats"));

            // The verbose output is a list of strings that looks like, note the innards of the list is straight up string:
            // [{"name":"summary","type:[requirements]:":{"Start":[]},"params":{"pretty":"Pretty print the output."}}, {"name":"bulk-load","type:[requirements]:":{"Start":[]},"params":{"See bulk loading documentation":"https://docs.aerospike.com/graph/usage/bulk-loader"}}]
            for (final Object output: verboseOutput) {
                final String outputString = (String) output;
                final List<String> infoPieces = List.of(outputString.split(",")).stream().map(String::trim).collect(Collectors.toList());
                if (infoPieces.size() != 3) {
                    Assert.fail("Error, split " + outputString + " into " + infoPieces.size() + " pieces via ',' delimiter, expected 3 pieces.");
                }

                switch (infoPieces.get(0)) {
                    case "{\"name\":\"summary\"":
                        Assert.assertEquals(infoPieces.get(1), "\"type:[requirements]:\":{\"Start\":[]}");
                        Assert.assertEquals(infoPieces.get(2), "\"params\":{\"pretty\":\"Pretty print the output.\"}}");
                        break;
                    case "{\"name\":\"bulk-load\"":
                        Assert.assertEquals(infoPieces.get(1), "\"type:[requirements]:\":{\"Start\":[]}");
                        Assert.assertEquals(infoPieces.get(2), "\"params\":{\"See bulk loading documentation\":\"https://docs.aerospike.com/graph/usage/bulk-loader\"}}");
                        break;
                    case "{\"name\":\"usage-stats\"":
                        Assert.assertEquals(infoPieces.get(1), "\"type:[requirements]:\":{\"Start\":[]}");
                        Assert.assertEquals(infoPieces.get(2), "\"params\":{}}");
                        break;
                    default:
                        Assert.fail("Error, expected first piece of " + outputString +
                                " to be \"name\":\"summary\" or \"name\":\"bulk-load\". Instead found " + infoPieces.get(0));
                        break;
                }
            }
        }
    }
}
