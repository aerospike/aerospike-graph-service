package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.Set;
import java.util.stream.Collectors;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.INDEX_METADATA_UPDATE_FREQUENCY;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES;

public class TestGraphStepHasContainerNumericBounds {
    public void testTraversals(final GraphTraversalSource g) {
        final Set<Integer> age = g.V().toSet().stream().filter(v -> v.property("age").isPresent()).map(v ->
                (Integer) v.property("age").value()).collect(Collectors.toSet());
        Assert.assertTrue(age.contains(27));
        Assert.assertTrue(age.size() > 3);

        final Set<Integer> ageGte26 = g.V().has("age", P.gte(26)).toSet().stream().map(v ->
                (Integer) v.property("age").value()).collect(Collectors.toSet());
        final Set<Integer> ageGt26 = g.V().has("age", P.gt(26)).toSet().stream().map(v ->
                (Integer) v.property("age").value()).collect(Collectors.toSet());
        final Set<Integer> ageGte27 = g.V().has("age", P.gte(27)).toSet().stream().map(v ->
                (Integer) v.property("age").value()).collect(Collectors.toSet());
        final Set<Integer> ageGt27 = g.V().has("age", P.gt(27)).toSet().stream().map(v ->
                (Integer) v.property("age").value()).collect(Collectors.toSet());
        final Set<Integer> ageGte28 = g.V().has("age", P.gte(28)).toSet().stream().map(v ->
                (Integer) v.property("age").value()).collect(Collectors.toSet());
        final Set<Integer> ageGt28 = g.V().has("age", P.gt(28)).toSet().stream().map(v ->
                (Integer) v.property("age").value()).collect(Collectors.toSet());

        final Set<Integer> ageGte26Expected = age.stream().filter(a -> a >= 26).collect(Collectors.toSet());
        final Set<Integer> ageGt26Expected = age.stream().filter(a -> a > 26).collect(Collectors.toSet());
        final Set<Integer> ageGte27Expected = age.stream().filter(a -> a >= 27).collect(Collectors.toSet());
        final Set<Integer> ageGt27Expected = age.stream().filter(a -> a > 27).collect(Collectors.toSet());
        final Set<Integer> ageGte28Expected = age.stream().filter(a -> a >= 28).collect(Collectors.toSet());
        final Set<Integer> ageGt28Expected = age.stream().filter(a -> a > 28).collect(Collectors.toSet());

        Assert.assertEquals(ageGt28Expected, ageGt28);
        Assert.assertEquals(ageGte28Expected, ageGte28);
        Assert.assertEquals(ageGt27Expected, ageGt27);
        Assert.assertEquals(ageGte27Expected, ageGte27);
        Assert.assertEquals(ageGt26Expected, ageGt26);
        Assert.assertEquals(ageGte26Expected, ageGte26);

        final Set<Integer> ageLte26 = g.V().has("age", P.lte(26)).toSet().stream().map(v ->
                (Integer) v.property("age").value()).collect(Collectors.toSet());
        final Set<Integer> ageLt26 = g.V().has("age", P.lt(26)).toSet().stream().map(v ->
                (Integer) v.property("age").value()).collect(Collectors.toSet());
        final Set<Integer> ageLte27 = g.V().has("age", P.lte(27)).toSet().stream().map(v ->
                (Integer) v.property("age").value()).collect(Collectors.toSet());
        final Set<Integer> ageLt27 = g.V().has("age", P.lt(27)).toSet().stream().map(v ->
                (Integer) v.property("age").value()).collect(Collectors.toSet());
        final Set<Integer> ageLte28 = g.V().has("age", P.lte(28)).toSet().stream().map(v ->
                (Integer) v.property("age").value()).collect(Collectors.toSet());
        final Set<Integer> ageLt28 = g.V().has("age", P.lt(28)).toSet().stream().map(v ->
                (Integer) v.property("age").value()).collect(Collectors.toSet());

        final Set<Integer> ageLte26Expected = age.stream().filter(a -> a <= 26).collect(Collectors.toSet());
        final Set<Integer> ageLt26Expected = age.stream().filter(a -> a < 26).collect(Collectors.toSet());
        final Set<Integer> ageLte27Expected = age.stream().filter(a -> a <= 27).collect(Collectors.toSet());
        final Set<Integer> ageLt27Expected = age.stream().filter(a -> a < 27).collect(Collectors.toSet());
        final Set<Integer> ageLte28Expected = age.stream().filter(a -> a <= 28).collect(Collectors.toSet());
        final Set<Integer> ageLt28Expected = age.stream().filter(a -> a < 28).collect(Collectors.toSet());

        Assert.assertEquals(ageLt28Expected, ageLt28);
        Assert.assertEquals(ageLte28Expected, ageLte28);
        Assert.assertEquals(ageLt27Expected, ageLt27);
        Assert.assertEquals(ageLte27Expected, ageLte27);
        Assert.assertEquals(ageLt26Expected, ageLt26);
        Assert.assertEquals(ageLte26Expected, ageLte26);
    }

    @Test
    public void testNumericHasIndexBounds() throws Exception {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(VERTEX_PROPERTY_INDEXES.toLowerCase(), "age");
        config.setProperty(INDEX_METADATA_UPDATE_FREQUENCY.toLowerCase(), 1);
        try (FireflyGraph graph = FireflyGraph.open(config)){
            graph.getBaseGraph().dropDatabase(graph, false);
            GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
            final GraphTraversalSource g = graph.traversal();
            Thread.sleep(10);
            testTraversals(g);
        } finally {
            try (FireflyGraph graph = FireflyGraph.open(config)) {
                graph.getBaseGraph().dropDatabase(graph, true);
            }
        }
    }

    @Test
    public void testNumericHasNonIndexBounds() throws InterruptedException {
        Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(VERTEX_PROPERTY_INDEXES.toLowerCase(), "");
        config.setProperty(INDEX_METADATA_UPDATE_FREQUENCY.toLowerCase(), 1);
        try (FireflyGraph graph = FireflyGraph.open(config)){
            graph.getBaseGraph().dropDatabase(graph, true);
            GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
            final GraphTraversalSource g = graph.traversal();
            Thread.sleep(10);
            testTraversals(g);
        } finally {
            try (FireflyGraph graph = FireflyGraph.open(config)) {
                graph.getBaseGraph().dropDatabase(graph, true);
            }
        }
    }
}
