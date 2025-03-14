package com.aerospike.firefly.io;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class TestTraversalOptions extends AbstractFireflySuite {

    @Test
    public void testEdgeParallelRead() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        GraphTraversalSource g = graph.traversal();
        // Should work
        g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 1).V().outE().next();
        g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 100).V().outE().next();
        g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, "1").V().outE().next();
        g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, "100").V().outE().next();

        // Should throw.
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be greater than 1. 0 is less than 1.",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 0).V().outE().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be greater than 1. -1 is less than 1.",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, -1).V().outE().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be greater than 1. 0 is less than 1.",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 0L).V().outE().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be greater than 1. -1 is less than 1.",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, -1L).V().outE().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be greater than 1. 0 is less than 1.",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, "0").V().outE().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be greater than 1. -1 is less than 1.",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, "-1").V().outE().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be an integer or integer string. ? is of type java.lang.String",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, "?").V().outE().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be an integer or integer string. false is of type java.lang.Boolean",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, false).V().outE().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be an integer or integer string. 1.0 is of type java.lang.Double",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 1.0).V().outE().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be an integer or integer string. 1.0 is of type java.lang.Float",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 1.0f).V().outE().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be an integer or integer string. 1.0 is of type java.lang.Double",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 1.0d).V().outE().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be an integer or integer string. [1] is of type java.collections.List",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, List.of(1)).V().outE().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be an integer or integer string. {\"?\":1} is of type java.collections.Map",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, Map.of("?", 1)).V().outE().next());
    }

    // TODO: Functional correctness test for parallel reading.

    @Override
    protected boolean clearData() {
        return true;
    }
}
