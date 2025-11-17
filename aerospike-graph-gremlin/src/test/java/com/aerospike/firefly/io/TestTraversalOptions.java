package com.aerospike.firefly.io;

import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

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

    @Test
    public void testVertexParallelRead() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        GraphTraversalSource g = graph.traversal();
        // Should work
        g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 1).V().out().next();
        g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 100).V().out().next();
        g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, "1").V().out().next();
        g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, "100").V().out().next();

        // Should throw.
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be greater than 1. 0 is less than 1.",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 0).V().out().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be greater than 1. -1 is less than 1.",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, -1).V().out().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be greater than 1. 0 is less than 1.",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 0L).V().out().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be greater than 1. -1 is less than 1.",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, -1L).V().out().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be greater than 1. 0 is less than 1.",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, "0").V().out().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be greater than 1. -1 is less than 1.",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, "-1").V().out().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be an integer or integer string. ? is of type java.lang.String",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, "?").V().out().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be an integer or integer string. false is of type java.lang.Boolean",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, false).V().out().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be an integer or integer string. 1.0 is of type java.lang.Double",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 1.0).V().out().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be an integer or integer string. 1.0 is of type java.lang.Float",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 1.0f).V().out().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be an integer or integer string. 1.0 is of type java.lang.Double",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 1.0d).V().out().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be an integer or integer string. [1] is of type java.collections.List",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, List.of(1)).V().out().next());
        Assert.assertThrows("Invalid value for aerospike.graph.parallelize option. Must be an integer or integer string. {\"?\":1} is of type java.collections.Map",
                ConfigurationRuntimeException.class, () -> g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, Map.of("?", 1)).V().out().next());
    }

    @Test
    public void testFunctionalCorrectness() {
        final GraphTraversalSource g = graph.traversal();
        g.V().drop().iterate();
        List<Vertex> vertices = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            vertices.add(g.addV().property(T.id, i).property("i", i).next());
        }
        for (int i = 0; i < 1998; i++) {
            g.addE("knows").property("i", i).from(vertices.get(i)).to(vertices.get(i + 1)).next();
        }
        for (int i = 1990; i < 2000; i++) {
            for (int j = 0; j < 10_000; j++) {
                g.addE("knows").property("ij", i + j).from(vertices.get(i)).to(vertices.get(0)).next();
            }
        }

        List<Vertex> vresult = g.V().out().toList();
        List<Vertex> vresult2 = g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 2).V().out().toList();
        List<Vertex> vresult4 = g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 4).V().out().toList();
        List<Vertex> vresult16 = g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 16).V().out().toList();
        List<Edge> eresult = g.V().outE().toList();
        List<Edge> eresult2 = g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 2).V().outE().toList();
        List<Edge> eresult4 = g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 4).V().outE().toList();
        List<Edge> eresult16 = g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 16).V().outE().toList();

        Assert.assertEquals(vresult.size(), vresult2.size());
        Assert.assertEquals(vresult.size(), vresult4.size());
        Assert.assertEquals(vresult.size(), vresult16.size());
        Assert.assertEquals(eresult.size(), eresult2.size());
        Assert.assertEquals(eresult.size(), eresult4.size());
        Assert.assertEquals(eresult.size(), eresult16.size());

        vresult = g.V().out().has("i", 10).toList();
        vresult2 = g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 2).V().out().has("i", 10).toList();
        vresult4 = g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 4).V().out().has("i", 10).toList();
        vresult16 = g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 16).V().out().has("i", 10).toList();
        eresult = g.V().outE().has("i", 10).toList();
        eresult2 = g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 2).V().outE().has("i", 10).toList();
        eresult4 = g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 4).V().outE().has("i", 10).toList();
        eresult16 = g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 16).V().outE().has("i", 10).toList();

        Assert.assertEquals(vresult.size(), vresult2.size());
        Assert.assertEquals(vresult.size(), vresult4.size());
        Assert.assertEquals(vresult.size(), vresult16.size());
        Assert.assertEquals(eresult.size(), eresult2.size());
        Assert.assertEquals(eresult.size(), eresult4.size());
        Assert.assertEquals(eresult.size(), eresult16.size());

        vresult = g.V().out().has("i", 1999).toList();
        vresult2 = g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 2).V().out().has("i", 1999).toList();
        vresult4 = g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 4).V().out().has("i", 1999).toList();
        vresult16 = g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 16).V().out().has("i", 1999).toList();
        eresult = g.V().outE().has("ij", 10000).toList();
        eresult2 = g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 2).V().outE().has("ij", 10000).toList();
        eresult4 = g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 4).V().outE().has("ij", 10000).toList();
        eresult16 = g.with(ConfigurationHelper.TraversalOptions.PARALLELIZE, 16).V().outE().has("ij", 10000).toList();

        Assert.assertEquals(vresult.size(), vresult2.size());
        Assert.assertEquals(vresult.size(), vresult4.size());
        Assert.assertEquals(vresult.size(), vresult16.size());
        Assert.assertEquals(eresult.size(), eresult2.size());
        Assert.assertEquals(eresult.size(), eresult4.size());
        Assert.assertEquals(eresult.size(), eresult16.size());
    }

    @Test
    public void testTraversalOptionsInChildTraversal() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        GraphTraversalSource g = graph.traversal();
        g = g.with("PARALLELIZE", 1);

        // Root traversal that owns a child
        GraphTraversal.Admin<?, ?> root = g.V().filter(__.out()).asAdmin();

        // Extract the real child traversal from the parent step
        TraversalParent parent = (TraversalParent) root.getSteps().stream()
                .filter(TraversalParent.class::isInstance)
                .findFirst().orElseThrow();
        Traversal.Admin<?, ?> child = parent.getLocalChildren().get(0);

        // Verify that the child has no options but the root does
        Assert.assertFalse(child.getStrategies().getStrategy(OptionsStrategy.class)
                .map(os -> os.getOptions().containsKey("PARALLELIZE")).orElse(false));
        Assert.assertTrue(root.getStrategies().getStrategy(OptionsStrategy.class)
                .map(os -> os.getOptions().containsKey("PARALLELIZE")).orElse(false));

        // Verify that PARALLELIZE=1 is taken from the root
        Optional<Integer> opt = ConfigurationHelper.getTraversalOptionInteger("PARALLELIZE", child, 0, 8);
        Assert.assertTrue(opt.isPresent());
        Assert.assertEquals(1, opt.get().intValue());
    }

    @Override
    protected boolean clearData() {
        return true;
    }
}
