package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.exceptions.GraphError;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.apache.tinkerpop.gremlin.process.traversal.Merge.onCreate;
import static org.apache.tinkerpop.gremlin.process.traversal.Merge.onMatch;

public class FireflyMergeEdgeStepNSupDisabledTest {
    @Test
    public void testNsupDisabledErrorMessage() {
        try (final FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            try {
                graph.traversal().addV().property(T.id, "vertex1").iterate();
                graph.traversal().addV().property(T.id, "vertex2").iterate();
                graph.traversal().mergeE(Map.of(
                                T.label, "connected",
                                Direction.OUT, "vertex1",
                                Direction.IN, "vertex2"))
                        .option(onCreate, Map.of("state", "created"))
                        .option(onMatch, Map.of("state", "matched"))
                        .toList();
            } catch (Exception e) {
                Assert.assertEquals(e.getMessage(), GraphError.getMessage(GraphError.NSUP_DISABLED));
            }
        }
    }
}
