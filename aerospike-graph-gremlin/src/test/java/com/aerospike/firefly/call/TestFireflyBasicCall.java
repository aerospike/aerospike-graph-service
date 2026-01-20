package com.aerospike.firefly.call;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
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

            Assert.assertEquals(Set.of(
                    "aerospike.graph.admin.metadata.summary",
                    "summary",
                    "aerospike.graph.admin.metadata.usage",
                    "usage-stats",
                    "aerospike.graph.admin.index.create",
                    "aerospike.graph.admin.index.drop",
                    "aerospike.graph.admin.index.list",
                    "aerospike.graph.admin.index.status",
                    "aerospike.graph.admin.index.cardinality",
                    "aerospike.graph.admin.reserved.info",
                    "aerospike.graph.admin.metadata.version",
                    "aerospike.graph.admin.metadata.config",
                    "aerospike.graph.admin.metadata.set-config",
                    "aerospike.graph.admin.rbac-jwt.issue-token",
                    "aerospike.graph.admin.query.abort",
                    "aerospike.graph.admin.cache.reset",
                    "aerospike.graph.admin.cache.set_mode",
                    "aerospike.graph.admin.cache.status"
            ), new HashSet<>(normalOutput));

            // The verbose output is a list of strings that looks like, note the innards of the list is straight up string:
            // [{"name":"aerospike.graph.admin.metadata.summary","type:[requirements]:":{"Start":[]},"params":{"pretty":"Pretty print the output."}}, {"name":"aerospike.graphloader.admin.bulk-load.load","type:[requirements]:":{"Start":[]},"params":{"See bulk loading documentation":"https://docs.aerospike.com/graph/usage/bulk-loader"}}]
            for (final Object output : verboseOutput) {
                final String outputString = (String) output;
                final List<String> infoPieces = List.of(outputString.split(",")).stream().map(String::trim).collect(Collectors.toList());

                switch (infoPieces.get(0)) {
                    case "{\"name\":\"aerospike.graph.admin.metadata.summary\"":
                    case "{\"name\":\"summary\"":
                        Assert.assertEquals(infoPieces.get(1), "\"type:[requirements]:\":{\"Start\":[]}");
                        Assert.assertEquals(infoPieces.get(2), "\"params\":{\"pretty\":\"Pretty print the output.\"}}");
                        break;
                    case "{\"name\":\"aerospike.graphloader.admin.bulk-load.load\"":
                    case "{\"name\":\"bulk-load\"":
                        Assert.assertEquals(infoPieces.get(1), "\"type:[requirements]:\":{\"Start\":[]}");
                        Assert.assertEquals(infoPieces.get(2), "\"params\":{\"See bulk loading documentation\":\"https://aerospike.com/docs/graph/data-loading/standalone#configuration-options\"}}");
                        break;
                    case "{\"name\":\"aerospike.graphloader.admin.bulk-load.errors\"":
                    case "{\"name\":\"get-bulk-load-errors\"":
                    case "{\"name\":\"aerospike.graphloader.admin.bulk-load.error-count\"":
                    case "{\"name\":\"get-bulk-load-error-count\"":
                    case "{\"name\":\"aerospike.graph.admin.metadata.version\"":
                    case "{\"name\":\"aerospike.graph.admin.metadata.config\"":
                    case "{\"name\":\"aerospike.graph.admin.metadata.set-config\"":
                        if (infoPieces.size() > 1) {
                            Assert.assertEquals(infoPieces.get(1), "\"type:[requirements]:\":{\"Start\":[]}");
                            Assert.assertEquals(infoPieces.get(2), "\"params\":{}}");
                        }

                    case "{\"name\":\"aerospike.graph.admin.reserved.info\"":
                    case "{\"name\":\"aerospike.graph.admin.index.list\"":
                    case "{\"name\":\"aerospike.graph.admin.index.cardinality\"":
                    case "{\"name\":\"aerospike.graph.admin.query.abort\"":
                    case "{\"name\":\"aerospike.graph.admin.cache.reset\"":
                    case "{\"name\":\"aerospike.graph.admin.cache.status\"":
                        Assert.assertEquals(infoPieces.get(1), "\"type:[requirements]:\":{\"Start\":[]}");
                        Assert.assertEquals(infoPieces.get(2), "\"params\":{}}");
                        break;
                    case "{\"name\":\"aerospike.graph.admin.cache.set_mode\"":
                        Assert.assertEquals(infoPieces.get(1), "\"type:[requirements]:\":{\"Start\":[]}");
                        // set_mode has params for mode and cache_weight
                        Assert.assertTrue(infoPieces.get(2).contains("\"params\":{"));
                        break;
                    case "{\"name\":\"aerospike.graph.admin.metadata.usage\"":
                    case "{\"name\":\"usage-stats\"":
                        Assert.assertEquals(infoPieces.get(1), "\"type:[requirements]:\":{\"Start\":[]}");
                        Assert.assertEquals(infoPieces.get(2), "\"params\":{\"since\":\"Return usage stats since a certain date in format 'yyyy-MM-dd'.\"}}");
                        break;
                    case "{\"name\":\"aerospike.graph.admin.index.create\"":
                        Assert.assertEquals(infoPieces.get(1), "\"type:[requirements]:\":{\"Start\":[]}");
                        Assert.assertEquals(infoPieces.get(2), "\"params\":{\"property_key\":\"The property key to create the index on. '~label' can be used to create an index on labels.\"");
                        Assert.assertEquals(infoPieces.get(3), "\"element_type\":\"The type of element to create the index on. Only 'vertex' is currently supported.\"");
                        Assert.assertEquals(infoPieces.get(4), "\"index_type\":\"Optional parameter and not supported for labels. The type of index to create on the property key. Value must be 'string' or 'numeric'. If not specified both types are created.\"}}");
                        break;
                    case "{\"name\":\"aerospike.graph.admin.index.drop\"":
                        Assert.assertEquals(infoPieces.get(1), "\"type:[requirements]:\":{\"Start\":[]}");
                        Assert.assertEquals(infoPieces.get(2), "\"params\":{\"property_key\":\"The property key to drop the index on. '~label' can be used to drop an index on labels.\"");
                        Assert.assertEquals(infoPieces.get(3), "\"element_type\":\"The type of element to drop the index on. Only 'vertex' is currently supported.\"");
                        Assert.assertEquals(infoPieces.get(4), "\"index_type\":\"Optional parameter and not supported for labels. The type of index to drop on the property key. Value must be 'string' or 'numeric'. If not specified both types are dropped.\"}}");
                        break;
                    case "{\"name\":\"aerospike.graph.admin.index.status\"":
                        Assert.assertEquals(infoPieces.get(1), "\"type:[requirements]:\":{\"Start\":[]}");
                        Assert.assertEquals(infoPieces.get(2), "\"params\":{\"property_key\":\"The property key to get the index status of. '~label' can be used to get the status of an index on labels.\"");
                        Assert.assertEquals(infoPieces.get(3), "\"element_type\":\"The type of element to get the index status of. Only 'vertex' is currently supported.\"}}");
                        break;
                    case "{\"name\":\"aerospike.graph.admin.rbac-jwt.issue-token\"":
                        Assert.assertEquals(infoPieces.get(1), "\"type:[requirements]:\":{\"Start\":[]}");
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
