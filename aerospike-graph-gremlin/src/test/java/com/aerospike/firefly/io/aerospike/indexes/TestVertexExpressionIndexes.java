package com.aerospike.firefly.io.aerospike.indexes;

import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.admin.Admin;
import com.aerospike.firefly.io.aerospike.query.FireflyExpressionIndex;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestVertexExpressionIndexes {
    private static final String SCAN_ENABLED_KEY = "aerospike.graph.scan.enabled";
    private static final String EXPRESSION_INDEX_KEY = "aerospike.graph.index.vertex.compound";

    private Configuration config;

    @BeforeClass
    public static void beforeAll() {
        final Configuration initConfig = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph initGraph = FireflyGraph.open(initConfig)) {
            initGraph.getBaseGraph().dropDatabase(initGraph, true);
        }
    }

    @Before
    public void setUp() {
        config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(SCAN_ENABLED_KEY, false);
        config.clearProperty(EXPRESSION_INDEX_KEY);
    }

    @After
    public void tearDown() {
        final Configuration cleanupConfig = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph graph = FireflyGraph.open(cleanupConfig)) {
            graph.getBaseGraph().dropDatabase(graph, true);
        }
    }

    private static void waitForExpressionIndex(final FireflyGraph graph, final String indexConfigString) {
        final long startTime = System.currentTimeMillis();
        final String[] indexConfigs = indexConfigString.split(";");
        for (final String indexConfig : indexConfigs) {
            final FireflyExpressionIndex expressionIndex = FireflyExpressionIndex.fromConfigString(
                    graph.getBaseGraph(), indexConfig);
            final String indexName = expressionIndex.getName();
            Map<String, Long> indexStatus = null;
            while (indexStatus == null || !indexStatus.get("percent_complete").equals(100L)) {
                try {
                    indexStatus = Admin.Index.getIndexStatus(graph, indexName);
                } catch (final IllegalStateException e) {
                    // Index not found yet, keep waiting
                }
                if (System.currentTimeMillis() > startTime + 30000) {
                    Assert.fail("Timed out waiting for expression index creation: " + indexName);
                }
                try {
                    Thread.sleep(100);
                } catch (final InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
            }
        }
        // Refresh metadata so the query planner sees the new indexes
        graph.fireflyIndexMetadata.updateMetadata();
    }

    @Test
    public void testExpressionIndexWithLabelAndEquality() {
        final String indexConfig = "~label:person,status:active";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("person").property("status", "active").property("name", "Alice").iterate();
            g.addV("person").property("status", "active").property("name", "Bob").iterate();
            g.addV("person").property("status", "inactive").property("name", "Charlie").iterate();
            g.addV("animal").property("status", "active").property("name", "Dog").iterate();
            g.addV("person").property("name", "Eve").iterate();

            List<Vertex> results = g.V().hasLabel("person").has("status", "active").toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithNumericGte() {
        final String indexConfig = "~label:product,category:electronics,price:~gte(100)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("product").property("category", "electronics").property("price", 150).property("name", "Phone").iterate();
            g.addV("product").property("category", "electronics").property("price", 200).property("name", "Laptop").iterate();
            g.addV("product").property("category", "electronics").property("price", 50).property("name", "Charger").iterate();
            g.addV("product").property("category", "clothing").property("price", 150).property("name", "Jacket").iterate();
            g.addV("product").property("category", "electronics").property("price", 100).property("name", "Headphones").iterate();

            List<Vertex> results = g.V().hasLabel("product").has("category", "electronics").has("price", P.gte(100)).toList();
            Assert.assertEquals(3, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithNumericGt() {
        final String indexConfig = "~label:item,count:~gt(5)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("item").property("count", 10).property("name", "A").iterate();
            g.addV("item").property("count", 6).property("name", "B").iterate();
            g.addV("item").property("count", 5).property("name", "C").iterate();
            g.addV("item").property("count", 3).property("name", "D").iterate();
            g.addV("other").property("count", 10).property("name", "E").iterate();

            List<Vertex> results = g.V().hasLabel("item").has("count", P.gt(5)).toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithNumericLte() {
        final String indexConfig = "~label:score,value:~lte(50)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("score").property("value", 50).property("player", "A").iterate();
            g.addV("score").property("value", 30).property("player", "B").iterate();
            g.addV("score").property("value", 51).property("player", "C").iterate();
            g.addV("score").property("value", 100).property("player", "D").iterate();

            List<Vertex> results = g.V().hasLabel("score").has("value", P.lte(50)).toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithNumericLt() {
        final String indexConfig = "~label:rating,stars:~lt(4)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("rating").property("stars", 1).property("review", "bad").iterate();
            g.addV("rating").property("stars", 3).property("review", "ok").iterate();
            g.addV("rating").property("stars", 4).property("review", "good").iterate();
            g.addV("rating").property("stars", 5).property("review", "great").iterate();

            List<Vertex> results = g.V().hasLabel("rating").has("stars", P.lt(4)).toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithSearchKeyNumeric() {
        final String indexConfig = "~label:employee,department:engineering,level:~n*";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("employee").property("department", "engineering").property("level", 1).property("name", "Junior1").iterate();
            g.addV("employee").property("department", "engineering").property("level", 2).property("name", "Mid1").iterate();
            g.addV("employee").property("department", "engineering").property("level", 2).property("name", "Mid2").iterate();
            g.addV("employee").property("department", "engineering").property("level", 3).property("name", "Senior1").iterate();
            g.addV("employee").property("department", "sales").property("level", 2).property("name", "SalesMid").iterate();

            List<Vertex> results = g.V().hasLabel("employee").has("department", "engineering").has("level", 2).toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithSearchKeyString() {
        final String indexConfig = "~label:user,verified:true,country:~s*";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("user").property("verified", "true").property("country", "USA").property("name", "Alice").iterate();
            g.addV("user").property("verified", "true").property("country", "USA").property("name", "Bob").iterate();
            g.addV("user").property("verified", "true").property("country", "Canada").property("name", "Charlie").iterate();
            g.addV("user").property("verified", "false").property("country", "USA").property("name", "Dave").iterate();

            List<Vertex> results = g.V().hasLabel("user").has("verified", "true").has("country", "USA").toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithMultiplePredicates() {
        final String indexConfig = "~label:order,status:completed,priority:~gte(2),region:~eq(west)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("order").property("status", "completed").property("priority", 3).property("region", "west").property("id", "1").iterate();
            g.addV("order").property("status", "completed").property("priority", 2).property("region", "west").property("id", "2").iterate();
            g.addV("order").property("status", "completed").property("priority", 1).property("region", "west").property("id", "3").iterate();
            g.addV("order").property("status", "completed").property("priority", 3).property("region", "east").property("id", "4").iterate();
            g.addV("order").property("status", "pending").property("priority", 3).property("region", "west").property("id", "5").iterate();

            List<Vertex> results = g.V().hasLabel("order")
                    .has("status", "completed")
                    .has("priority", P.gte(2))
                    .has("region", "west")
                    .toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexNoMatchingVertices() {
        final String indexConfig = "~label:ghost,exists:yes";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("ghost").property("exists", "no").property("name", "Casper").iterate();
            g.addV("other").property("exists", "yes").property("name", "Something").iterate();

            List<Vertex> results = g.V().hasLabel("ghost").has("exists", "yes").toList();
            Assert.assertEquals(0, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithLongValues() {
        final String indexConfig = "~label:bigdata,size:~gte(1000000000)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("bigdata").property("size", 5000000000L).property("name", "huge").iterate();
            g.addV("bigdata").property("size", 1000000000L).property("name", "large").iterate();
            g.addV("bigdata").property("size", 500000000L).property("name", "medium").iterate();

            List<Vertex> results = g.V().hasLabel("bigdata").has("size", P.gte(1000000000L)).toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testMultipleExpressionIndexes() {
        final String indexConfig = "~label:typeA,foo:bar;~label:typeB,baz:qux";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("typeA").property("foo", "bar").property("name", "A1").iterate();
            g.addV("typeA").property("foo", "bar").property("name", "A2").iterate();
            g.addV("typeA").property("foo", "other").property("name", "A3").iterate();
            g.addV("typeB").property("baz", "qux").property("name", "B1").iterate();
            g.addV("typeB").property("baz", "qux").property("name", "B2").iterate();
            g.addV("typeB").property("baz", "other").property("name", "B3").iterate();

            List<Vertex> resultsA = g.V().hasLabel("typeA").has("foo", "bar").toList();
            Assert.assertEquals(2, resultsA.size());

            List<Vertex> resultsB = g.V().hasLabel("typeB").has("baz", "qux").toList();
            Assert.assertEquals(2, resultsB.size());
        }
    }

    @Test
    public void testExpressionIndexQueryWithAdditionalFilters() {
        final String indexConfig = "~label:widget,active:true";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("widget").property("active", "true").property("color", "red").property("size", 10).iterate();
            g.addV("widget").property("active", "true").property("color", "blue").property("size", 20).iterate();
            g.addV("widget").property("active", "true").property("color", "red").property("size", 30).iterate();
            g.addV("widget").property("active", "false").property("color", "red").property("size", 10).iterate();

            List<Vertex> results = g.V().hasLabel("widget").has("active", "true").has("color", "red").toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithIntegerAndLongEquivalence() {
        final String indexConfig = "~label:number,value:~gte(10)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("number").property("value", 15).property("type", "int").iterate();
            g.addV("number").property("value", 15L).property("type", "long").iterate();
            g.addV("number").property("value", 10).property("type", "boundary").iterate();
            g.addV("number").property("value", 5).property("type", "below").iterate();

            List<Vertex> resultsWithInt = g.V().hasLabel("number").has("value", P.gte(10)).toList();
            Assert.assertEquals(3, resultsWithInt.size());

            List<Vertex> resultsWithLong = g.V().hasLabel("number").has("value", P.gte(10L)).toList();
            Assert.assertEquals(3, resultsWithLong.size());
        }
    }

    @Test
    public void testExpressionIndexWithListCardinalityProperty() {
        final String indexConfig = "~label:person,status:active";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("person").property("status", "active")
                    .property(VertexProperty.Cardinality.list, "tag", "developer")
                    .property(VertexProperty.Cardinality.list, "tag", "java")
                    .property(VertexProperty.Cardinality.list, "tag", "backend")
                    .iterate();
            g.addV("person").property("status", "active")
                    .property(VertexProperty.Cardinality.list, "tag", "designer")
                    .property(VertexProperty.Cardinality.list, "tag", "frontend")
                    .iterate();
            g.addV("person").property("status", "inactive")
                    .property(VertexProperty.Cardinality.list, "tag", "developer")
                    .iterate();

            List<Vertex> results = g.V().hasLabel("person").has("status", "active").toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithGtQueryWithGte() {
        // Index created with gt(5), query with gte(6) - should match since gt(5) == gte(6) internally
        final String indexConfig = "~label:item,value:~gt(5)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("item").property("value", 10).property("name", "A").iterate();
            g.addV("item").property("value", 6).property("name", "B").iterate();
            g.addV("item").property("value", 5).property("name", "C").iterate();
            g.addV("item").property("value", 3).property("name", "D").iterate();

            // gt(5) is stored as gte(6), so querying with gte(6) should use the index
            List<Vertex> results = g.V().hasLabel("item").has("value", P.gte(6)).toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithGteQueryWithGt() {
        // Index created with gte(6), query with gt(5) - should match since gt(5) == gte(6) internally
        final String indexConfig = "~label:item,value:~gte(6)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("item").property("value", 10).property("name", "A").iterate();
            g.addV("item").property("value", 6).property("name", "B").iterate();
            g.addV("item").property("value", 5).property("name", "C").iterate();
            g.addV("item").property("value", 3).property("name", "D").iterate();

            // gte(6) is stored as gte(6), gt(5) is converted to gte(6), so they should match
            List<Vertex> results = g.V().hasLabel("item").has("value", P.gt(5)).toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithLtQueryWithLte() {
        // Index created with lt(5), query with lte(4) - should match since lt(5) == lte(4) internally
        final String indexConfig = "~label:item,value:~lt(5)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("item").property("value", 10).property("name", "A").iterate();
            g.addV("item").property("value", 5).property("name", "B").iterate();
            g.addV("item").property("value", 4).property("name", "C").iterate();
            g.addV("item").property("value", 2).property("name", "D").iterate();

            // lt(5) is stored as lte(4), so querying with lte(4) should use the index
            List<Vertex> results = g.V().hasLabel("item").has("value", P.lte(4)).toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithLteQueryWithLt() {
        // Index created with lte(4), query with lt(5) - should match since lt(5) == lte(4) internally
        final String indexConfig = "~label:item,value:~lte(4)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("item").property("value", 10).property("name", "A").iterate();
            g.addV("item").property("value", 5).property("name", "B").iterate();
            g.addV("item").property("value", 4).property("name", "C").iterate();
            g.addV("item").property("value", 2).property("name", "D").iterate();

            // lte(4) is stored as lte(4), lt(5) is converted to lte(4), so they should match
            List<Vertex> results = g.V().hasLabel("item").has("value", P.lt(5)).toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithSetCardinalityProperty() {
        final String indexConfig = "~label:product,available:yes";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("product").property("available", "yes")
                    .property(VertexProperty.Cardinality.set, "category", "electronics")
                    .property(VertexProperty.Cardinality.set, "category", "gadgets")
                    .property(VertexProperty.Cardinality.set, "category", "electronics") // duplicate, should be ignored
                    .iterate();
            g.addV("product").property("available", "yes")
                    .property(VertexProperty.Cardinality.set, "category", "clothing")
                    .iterate();
            g.addV("product").property("available", "no")
                    .property(VertexProperty.Cardinality.set, "category", "electronics")
                    .iterate();

            List<Vertex> results = g.V().hasLabel("product").has("available", "yes").toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithoutLabel() {
        // Expression index without label - just property-based filtering
        final String indexConfig = "status:active,tier:premium";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("user").property("status", "active").property("tier", "premium").property("name", "Alice").iterate();
            g.addV("account").property("status", "active").property("tier", "premium").property("name", "Bob").iterate();
            g.addV("user").property("status", "active").property("tier", "basic").property("name", "Charlie").iterate();
            g.addV("user").property("status", "inactive").property("tier", "premium").property("name", "Dave").iterate();

            // Should match both user and account with status=active and tier=premium
            List<Vertex> results = g.V().has("status", "active").has("tier", "premium").toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithoutLabelNumericPredicate() {
        // Expression index without label using numeric predicate
        final String indexConfig = "category:electronics,price:~gte(100)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("product").property("category", "electronics").property("price", 150).iterate();
            g.addV("item").property("category", "electronics").property("price", 200).iterate();
            g.addV("product").property("category", "electronics").property("price", 50).iterate();
            g.addV("product").property("category", "clothing").property("price", 150).iterate();

            List<Vertex> results = g.V().has("category", "electronics").has("price", P.gte(100)).toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithExplicitEq() {
        // Using explicit ~eq() in the index definition
        final String indexConfig = "~label:order,status:~eq(confirmed),region:~eq(north)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("order").property("status", "confirmed").property("region", "north").property("id", "1").iterate();
            g.addV("order").property("status", "confirmed").property("region", "north").property("id", "2").iterate();
            g.addV("order").property("status", "confirmed").property("region", "south").property("id", "3").iterate();
            g.addV("order").property("status", "pending").property("region", "north").property("id", "4").iterate();

            List<Vertex> results = g.V().hasLabel("order").has("status", "confirmed").has("region", "north").toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithExplicitEqNumeric() {
        // Using explicit ~eq() with numeric value
        final String indexConfig = "~label:sensor,type:temperature,reading:~eq(100)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("sensor").property("type", "temperature").property("reading", 100).property("id", "1").iterate();
            g.addV("sensor").property("type", "temperature").property("reading", 100).property("id", "2").iterate();
            g.addV("sensor").property("type", "temperature").property("reading", 99).property("id", "3").iterate();
            g.addV("sensor").property("type", "humidity").property("reading", 100).property("id", "4").iterate();

            List<Vertex> results = g.V().hasLabel("sensor").has("type", "temperature").has("reading", 100).toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithFiveFilters() {
        // Complex expression index with 5 different filters
        final String indexConfig = "~label:tx,st:ok,ty:buy,rg:us,pri:~gte(3)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("tx").property("st", "ok").property("ty", "buy").property("rg", "us").property("pri", 5).iterate();
            g.addV("tx").property("st", "ok").property("ty", "buy").property("rg", "us").property("pri", 3).iterate();
            g.addV("tx").property("st", "no").property("ty", "buy").property("rg", "us").property("pri", 5).iterate();
            g.addV("tx").property("st", "ok").property("ty", "buy").property("rg", "us").property("pri", 2).iterate();
            g.addV("tx").property("st", "ok").property("ty", "sell").property("rg", "us").property("pri", 5).iterate();

            List<Vertex> results = g.V().hasLabel("tx")
                    .has("st", "ok").has("ty", "buy").has("rg", "us").has("pri", P.gte(3))
                    .toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithFiveFiltersAlternate() {
        // Another complex expression index with 5 different filters
        final String indexConfig = "~label:ev,cat:conf,loc:us,yr:~gte(24),tier:pro";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("ev").property("cat", "conf").property("loc", "us").property("yr", 25).property("tier", "pro").iterate();
            g.addV("ev").property("cat", "conf").property("loc", "us").property("yr", 24).property("tier", "pro").iterate();
            g.addV("ev").property("cat", "work").property("loc", "us").property("yr", 25).property("tier", "pro").iterate();
            g.addV("ev").property("cat", "conf").property("loc", "us").property("yr", 23).property("tier", "pro").iterate();

            List<Vertex> results = g.V().hasLabel("ev")
                    .has("cat", "conf").has("loc", "us").has("yr", P.gte(24)).has("tier", "pro")
                    .toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithoutLabelAndSearchKey() {
        // Expression index without label but with a search key
        final String indexConfig = "department:engineering,level:~gte(3),employeeId:~n*";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("employee").property("department", "engineering").property("level", 5).property("employeeId", 1001).iterate();
            g.addV("contractor").property("department", "engineering").property("level", 3).property("employeeId", 1001).iterate();
            g.addV("employee").property("department", "engineering").property("level", 4).property("employeeId", 1002).iterate();
            g.addV("employee").property("department", "engineering").property("level", 2).property("employeeId", 1001).iterate();
            g.addV("employee").property("department", "sales").property("level", 5).property("employeeId", 1001).iterate();

            List<Vertex> results = g.V()
                    .has("department", "engineering")
                    .has("level", P.gte(3))
                    .has("employeeId", 1001)
                    .toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexMixedEqAndRange() {
        // Mix of explicit eq and range predicates
        final String indexConfig = "~label:metric,source:~eq(sensor),env:~eq(prod),value:~gte(50),thr:~lte(100)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("metric").property("source", "sensor").property("env", "prod")
                    .property("value", 75).property("thr", 80).property("id", "1").iterate();
            g.addV("metric").property("source", "sensor").property("env", "prod")
                    .property("value", 50).property("thr", 100).property("id", "2").iterate();
            g.addV("metric").property("source", "sensor").property("env", "prod")
                    .property("value", 40).property("thr", 80).property("id", "3").iterate();
            g.addV("metric").property("source", "sensor").property("env", "prod")
                    .property("value", 75).property("thr", 150).property("id", "4").iterate();
            g.addV("metric").property("source", "sensor").property("env", "stg")
                    .property("value", 75).property("thr", 80).property("id", "5").iterate();

            List<Vertex> results = g.V().hasLabel("metric")
                    .has("source", "sensor")
                    .has("env", "prod")
                    .has("value", P.gte(50))
                    .has("thr", P.lte(100))
                    .toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexDiscoveredWithoutConfig() {
        final String indexConfig = "~label:person,status:active";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);

        // First graph: create index and add data
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("person").property("status", "active").property("name", "Alice").iterate();
            g.addV("person").property("status", "active").property("name", "Bob").iterate();
            g.addV("person").property("status", "inactive").property("name", "Charlie").iterate();

            List<Vertex> results = g.V().hasLabel("person").has("status", "active").toList();
            Assert.assertEquals(2, results.size());
        }

        // Second graph: reopen WITHOUT the index in config - should still discover it
        config.clearProperty(EXPRESSION_INDEX_KEY);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            List<Vertex> results = g.V().hasLabel("person").has("status", "active").toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithNumericPredicateDiscoveredWithoutConfig() {
        final String indexConfig = "~label:product,category:electronics,price:~gte(100)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);

        // First graph: create index and add data
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("product").property("category", "electronics").property("price", 150).property("name", "Phone").iterate();
            g.addV("product").property("category", "electronics").property("price", 200).property("name", "Laptop").iterate();
            g.addV("product").property("category", "electronics").property("price", 50).property("name", "Cable").iterate();
            g.addV("product").property("category", "furniture").property("price", 300).property("name", "Desk").iterate();

            List<Vertex> results = g.V().hasLabel("product")
                    .has("category", "electronics")
                    .has("price", P.gte(100))
                    .toList();
            Assert.assertEquals(2, results.size());
        }

        // Second graph: reopen WITHOUT the index in config
        config.clearProperty(EXPRESSION_INDEX_KEY);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            List<Vertex> results = g.V().hasLabel("product")
                    .has("category", "electronics")
                    .has("price", P.gte(100))
                    .toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithSearchKeyDiscoveredWithoutConfig() {
        // Search key indexes (~n* or ~s*) only work with equality queries
        final String indexConfig = "~label:document,type:report,year:~n*";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);

        // First graph: create index and add data
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("document").property("type", "report").property("year", 2024).property("title", "Annual").iterate();
            g.addV("document").property("type", "report").property("year", 2024).property("title", "Summary").iterate();
            g.addV("document").property("type", "report").property("year", 2023).property("title", "Quarterly").iterate();
            g.addV("document").property("type", "memo").property("year", 2024).property("title", "Notice").iterate();

            List<Vertex> results = g.V().hasLabel("document")
                    .has("type", "report")
                    .has("year", 2024)
                    .toList();
            Assert.assertEquals(2, results.size());
        }

        // Second graph: reopen WITHOUT the index in config
        config.clearProperty(EXPRESSION_INDEX_KEY);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            List<Vertex> results = g.V().hasLabel("document")
                    .has("type", "report")
                    .has("year", 2024)
                    .toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testMultipleExpressionIndexesDiscoveredWithoutConfig() {
        final String indexConfig = "~label:user,role:admin;~label:order,status:pending,priority:~gte(1)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);

        // First graph: create indexes and add data
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("user").property("role", "admin").property("name", "Admin1").iterate();
            g.addV("user").property("role", "admin").property("name", "Admin2").iterate();
            g.addV("user").property("role", "guest").property("name", "Guest1").iterate();

            g.addV("order").property("status", "pending").property("priority", 3).property("id", "O1").iterate();
            g.addV("order").property("status", "pending").property("priority", 1).property("id", "O2").iterate();
            g.addV("order").property("status", "complete").property("priority", 5).property("id", "O3").iterate();

            Assert.assertEquals(2, g.V().hasLabel("user").has("role", "admin").toList().size());
            Assert.assertEquals(2, g.V().hasLabel("order").has("status", "pending").has("priority", P.gte(1)).toList().size());
        }

        // Second graph: reopen WITHOUT any index config
        config.clearProperty(EXPRESSION_INDEX_KEY);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            Assert.assertEquals(2, g.V().hasLabel("user").has("role", "admin").toList().size());
            Assert.assertEquals(2, g.V().hasLabel("order").has("status", "pending").has("priority", P.gte(1)).toList().size());
        }
    }

    @Test
    public void testHighestWeightIndexIsSelected() {
        // Create two indexes that both match the same query, but with different weights
        // Index 1: weight = 2 (label + status)
        // Index 2: weight = 3 (label + status + category)
        final String indexConfig = "~label:item,status:active;~label:item,status:active,category:electronics";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("item").property("status", "active").property("category", "electronics").property("name", "Phone").iterate();
            g.addV("item").property("status", "active").property("category", "electronics").property("name", "Laptop").iterate();
            g.addV("item").property("status", "active").property("category", "furniture").property("name", "Desk").iterate();
            g.addV("item").property("status", "inactive").property("category", "electronics").property("name", "Old TV").iterate();

            // Build HasContainers that match both indexes
            final List<HasContainer> hasContainers = new ArrayList<>();
            hasContainers.add(new HasContainer("~label", P.eq("item")));
            hasContainers.add(new HasContainer("status", P.eq("active")));
            hasContainers.add(new HasContainer("category", P.eq("electronics")));

            // Get the matching index - should be the heavier one (3 predicates)
            final Optional<FireflyIndexMetadata.ExpressionIndexInfo> matchingIndex =
                    graph.fireflyIndexMetadata.getMatchingExpressionIndex(hasContainers);

            Assert.assertTrue("Expected a matching index", matchingIndex.isPresent());

            // The heavier index (weight=3) should have no unused HasContainers
            // If the lighter index (weight=2) was selected, "category" would be unused
            Assert.assertEquals("Heavier index should use all HasContainers",
                    0, matchingIndex.get().hasContainers.size());

            // Verify the query works correctly
            List<Vertex> results = g.V().hasLabel("item")
                    .has("status", "active")
                    .has("category", "electronics")
                    .toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testHighestWeightIndexWithNumericPredicate() {
        // Index 1: weight = 2 (label + type)
        // Index 2: weight = 3 (label + type + priority with gte)
        final String indexConfig = "~label:task,type:bug;~label:task,type:bug,priority:~gte(1)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("task").property("type", "bug").property("priority", 3).property("title", "Fix crash").iterate();
            g.addV("task").property("type", "bug").property("priority", 1).property("title", "Fix typo").iterate();
            g.addV("task").property("type", "feature").property("priority", 5).property("title", "Add feature").iterate();

            // Build HasContainers that match both indexes
            final List<HasContainer> hasContainers = new ArrayList<>();
            hasContainers.add(new HasContainer("~label", P.eq("task")));
            hasContainers.add(new HasContainer("type", P.eq("bug")));
            hasContainers.add(new HasContainer("priority", P.gte(1)));

            final Optional<FireflyIndexMetadata.ExpressionIndexInfo> matchingIndex =
                    graph.fireflyIndexMetadata.getMatchingExpressionIndex(hasContainers);

            Assert.assertTrue("Expected a matching index", matchingIndex.isPresent());
            Assert.assertEquals("Heavier index should use all HasContainers",
                    0, matchingIndex.get().hasContainers.size());

            // Verify the query works
            List<Vertex> results = g.V().hasLabel("task")
                    .has("type", "bug")
                    .has("priority", P.gte(1))
                    .toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithCount() {
        final String indexConfig = "~label:product,category:electronics";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("product").property("category", "electronics").property("name", "Phone").property("inStock", true).iterate();
            g.addV("product").property("category", "electronics").property("name", "Laptop").property("inStock", true).iterate();
            g.addV("product").property("category", "electronics").property("name", "Tablet").property("inStock", false).iterate();
            g.addV("product").property("category", "furniture").property("name", "Desk").property("inStock", true).iterate();
            g.addV("product").property("category", "furniture").property("name", "Chair").property("inStock", true).iterate();

            // Query matches the expression index
            long count = g.V().hasLabel("product").has("category", "electronics").count().next();
            Assert.assertEquals(3, count);

            // Query uses expression index with additional filter on non-indexed property
            count = g.V().hasLabel("product").has("category", "electronics").has("inStock", true).count().next();
            Assert.assertEquals(2, count);
        }
    }

    @Test
    public void testExpressionIndexWithCountAndNumericPredicate() {
        final String indexConfig = "~label:employee,department:engineering,level:~gte(2)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("employee").property("department", "engineering").property("level", 1).property("name", "Junior").property("active", true).iterate();
            g.addV("employee").property("department", "engineering").property("level", 2).property("name", "Mid1").property("active", true).iterate();
            g.addV("employee").property("department", "engineering").property("level", 3).property("name", "Mid2").property("active", false).iterate();
            g.addV("employee").property("department", "engineering").property("level", 4).property("name", "Senior").property("active", true).iterate();
            g.addV("employee").property("department", "sales").property("level", 5).property("name", "Director").property("active", true).iterate();

            // Query matches the expression index exactly
            long count = g.V().hasLabel("employee")
                    .has("department", "engineering")
                    .has("level", P.gte(2))
                    .count().next();
            Assert.assertEquals(3, count);

            // Query uses expression index with additional filter on non-indexed property
            count = g.V().hasLabel("employee")
                    .has("department", "engineering")
                    .has("level", P.gte(2))
                    .has("active", true)
                    .count().next();
            Assert.assertEquals(2, count);
        }
    }

    @Test
    public void testExpressionIndexWithCountMultipleIndexes() {
        final String indexConfig = "~label:order,status:pending;~label:order,status:shipped";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("order").property("status", "pending").property("id", "O1").iterate();
            g.addV("order").property("status", "pending").property("id", "O2").iterate();
            g.addV("order").property("status", "shipped").property("id", "O3").iterate();
            g.addV("order").property("status", "shipped").property("id", "O4").iterate();
            g.addV("order").property("status", "shipped").property("id", "O5").iterate();
            g.addV("order").property("status", "delivered").property("id", "O6").iterate();

            Assert.assertEquals(2, (long) g.V().hasLabel("order").has("status", "pending").count().next());
            Assert.assertEquals(3, (long) g.V().hasLabel("order").has("status", "shipped").count().next());
        }
    }

    @Test
    public void testInvalidConfigLessThanTwoCriteria() {
        final String indexConfig = "~label:person";
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            FireflyExpressionIndex.fromConfigString(graph.getBaseGraph(), indexConfig);
            Assert.fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("less than 2 search criteria"));
        }
    }

    @Test
    public void testInvalidConfigMissingColonDelimiter() {
        final String indexConfig = "~label:person,statusactive";
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            FireflyExpressionIndex.fromConfigString(graph.getBaseGraph(), indexConfig);
            Assert.fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("':' must appear only once"));
        }
    }

    @Test
    public void testInvalidConfigTooManyColons() {
        final String indexConfig = "~label:person,status:active:extra";
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            FireflyExpressionIndex.fromConfigString(graph.getBaseGraph(), indexConfig);
            Assert.fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("':' must appear only once"));
        }
    }

    @Test
    public void testInvalidConfigReservedKeyPrefix() {
        final String indexConfig = "~label:person,~invalid:value";
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            FireflyExpressionIndex.fromConfigString(graph.getBaseGraph(), indexConfig);
            Assert.fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("'~' denoting reserved keys"));
        }
    }

    @Test
    public void testInvalidConfigLabelAsSearchKey() {
        final String indexConfig = "~label:~n*,status:active";
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            FireflyExpressionIndex.fromConfigString(graph.getBaseGraph(), indexConfig);
            Assert.fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("cannot be used as a search key"));
        }
    }

    @Test
    public void testInvalidConfigMultipleSearchKeys() {
        final String indexConfig = "~label:person,key1:~n*,key2:~s*";
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            FireflyExpressionIndex.fromConfigString(graph.getBaseGraph(), indexConfig);
            Assert.fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Only one search key"));
        }
    }

    @Test
    public void testInvalidConfigMalformedPredicate() {
        final String indexConfig = "~label:person,value:~gte50";
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            FireflyExpressionIndex.fromConfigString(graph.getBaseGraph(), indexConfig);
            Assert.fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid predicate format"));
        }
    }

    @Test
    public void testInvalidConfigMissingClosingParen() {
        final String indexConfig = "~label:person,value:~gte(50";
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            FireflyExpressionIndex.fromConfigString(graph.getBaseGraph(), indexConfig);
            Assert.fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Invalid predicate format"));
        }
    }

    @Test
    public void testInvalidConfigNonNumericForGte() {
        final String indexConfig = "~label:person,value:~gte(abc)";
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            FireflyExpressionIndex.fromConfigString(graph.getBaseGraph(), indexConfig);
            Assert.fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("requires a number value"));
        }
    }

    @Test
    public void testInvalidConfigUnknownOperator() {
        final String indexConfig = "~label:person,value:~foo(50)";
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            FireflyExpressionIndex.fromConfigString(graph.getBaseGraph(), indexConfig);
            Assert.fail("Expected IllegalArgumentException");
        } catch (final IllegalArgumentException e) {
            Assert.assertTrue(e.getMessage().contains("Unknown predicate operator"));
        }
    }

    // ==================== g.call() Service Tests ====================

    @Test
    public void testCompoundIndexServiceList() {
        final String indexConfig = "~label:person,status:active;~label:order,priority:~gte(1)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            // Call the list service
            final Object result = g.call("aerospike.graph.admin.compound-index.list").next();

            Assert.assertNotNull(result);
            Assert.assertTrue(result instanceof List);
            final List<String> indexList = (List<String>) result;
            Assert.assertEquals(2, indexList.size());
        }
    }

    @Test
    public void testCompoundIndexServiceListEmpty() {
        // No indexes configured
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            final Object result = g.call("aerospike.graph.admin.compound-index.list").next();

            Assert.assertNotNull(result);
            Assert.assertTrue(result instanceof List);
            final List<String> indexList = (List<String>) result;
            Assert.assertEquals(0, indexList.size());
        }
    }

    @Test
    public void testCompoundIndexServiceCreate() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            // Create index using the service
            final Object result = g.call("aerospike.graph.admin.compound-index.create")
                    .with("predicates", List.of("~label:customer", "tier:premium"))
                    .next();

            Assert.assertNotNull(result);
            Assert.assertTrue(result instanceof String);
            final String message = (String) result;
            Assert.assertTrue(message.contains("creation in progress"));

            // Wait for the index to be created
            waitForExpressionIndex(graph, "~label:customer,tier:premium");

            // Verify the index is in the list
            final List<String> indexList = (List<String>) g.call("aerospike.graph.admin.compound-index.list").next();
            Assert.assertEquals(1, indexList.size());

            // Verify we can query using the index
            g.addV("customer").property("tier", "premium").property("name", "Alice").iterate();
            g.addV("customer").property("tier", "premium").property("name", "Bob").iterate();
            g.addV("customer").property("tier", "basic").property("name", "Charlie").iterate();

            List<Vertex> results = g.V().hasLabel("customer").has("tier", "premium").toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testCompoundIndexServiceCreateWithNumericPredicate() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            // Create index with numeric predicate
            final Object result = g.call("aerospike.graph.admin.compound-index.create")
                    .with("predicates", List.of("~label:product", "category:electronics", "price:~gte(100)"))
                    .next();

            Assert.assertNotNull(result);
            Assert.assertTrue(((String) result).contains("creation in progress"));

            waitForExpressionIndex(graph, "~label:product,category:electronics,price:~gte(100)");

            // Verify index works
            g.addV("product").property("category", "electronics").property("price", 150).iterate();
            g.addV("product").property("category", "electronics").property("price", 50).iterate();

            List<Vertex> results = g.V().hasLabel("product")
                    .has("category", "electronics")
                    .has("price", P.gte(100))
                    .toList();
            Assert.assertEquals(1, results.size());
        }
    }

    @Test
    public void testCompoundIndexServiceCreateWithSearchKey() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            // Create index with search key
            final Object result = g.call("aerospike.graph.admin.compound-index.create")
                    .with("predicates", List.of("~label:employee", "department:engineering", "employeeId:~n*"))
                    .next();

            Assert.assertNotNull(result);
            Assert.assertTrue(((String) result).contains("creation in progress"));

            waitForExpressionIndex(graph, "~label:employee,department:engineering,employeeId:~n*");

            // Verify index works with search key equality
            g.addV("employee").property("department", "engineering").property("employeeId", 1001).iterate();
            g.addV("employee").property("department", "engineering").property("employeeId", 1002).iterate();
            g.addV("employee").property("department", "engineering").property("employeeId", 1001).iterate();

            List<Vertex> results = g.V().hasLabel("employee")
                    .has("department", "engineering")
                    .has("employeeId", 1001)
                    .toList();
            Assert.assertEquals(2, results.size());
        }
    }

    @Test
    public void testCompoundIndexServiceStatus() {
        final String indexConfig = "~label:event,type:conference";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            // Get the index name
            final List<String> indexList = (List<String>) g.call("aerospike.graph.admin.compound-index.list").next();
            Assert.assertEquals(1, indexList.size());
            final String indexName = indexList.get(0);

            // Get the status
            final Object result = g.call("aerospike.graph.admin.compound-index.status")
                    .with("index_name", indexName)
                    .next();

            Assert.assertNotNull(result);
            Assert.assertTrue(result instanceof Map);
            final Map<String, Long> status = (Map<String, Long>) result;
            Assert.assertEquals(Long.valueOf(100L), status.get("percent_complete"));
            Assert.assertTrue(status.containsKey("total_entries"));
            Assert.assertTrue(status.containsKey("total_used_bytes"));
            Assert.assertTrue(status.containsKey("load_time"));
        }
    }

    @Test
    public void testCompoundIndexServiceStatusNotFound() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            try {
                g.call("aerospike.graph.admin.compound-index.status")
                        .with("index_name", "nonexistent_index")
                        .next();
                Assert.fail("Expected IllegalStateException");
            } catch (final Exception e) {
                Assert.assertTrue(e.getMessage().contains("not found") || e.getCause().getMessage().contains("not found"));
            }
        }
    }

    @Test
    public void testCompoundIndexServiceDrop() {
        final String indexConfig = "~label:temp,status:pending";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);

        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            // Verify index exists
            List<String> indexList = (List<String>) g.call("aerospike.graph.admin.compound-index.list").next();
            Assert.assertEquals(1, indexList.size());
            final String indexName = indexList.get(0);

            // Drop the index
            final Object result = g.call("aerospike.graph.admin.compound-index.drop")
                    .with("index_name", indexName)
                    .next();

            Assert.assertNotNull(result);
            Assert.assertTrue(result instanceof String);
            Assert.assertTrue(((String) result).contains("dropped"));

            // Wait for index to be removed
            waitForExpressionIndexesCleared(graph);

            // Verify index is gone
            indexList = (List<String>) g.call("aerospike.graph.admin.compound-index.list").next();
            Assert.assertEquals(0, indexList.size());
        }
    }

    @Test
    public void testCompoundIndexServiceCreateAndDropCycle() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            // Initially no indexes
            List<String> indexList = (List<String>) g.call("aerospike.graph.admin.compound-index.list").next();
            Assert.assertEquals(0, indexList.size());

            // Create an index
            g.call("aerospike.graph.admin.compound-index.create")
                    .with("predicates", List.of("~label:session", "active:true"))
                    .next();

            waitForExpressionIndex(graph, "~label:session,active:true");

            // Verify it exists
            indexList = (List<String>) g.call("aerospike.graph.admin.compound-index.list").next();
            Assert.assertEquals(1, indexList.size());
            final String indexName = indexList.get(0);

            // Check status
            final Map<String, Long> status = (Map<String, Long>) g.call("aerospike.graph.admin.compound-index.status")
                    .with("index_name", indexName)
                    .next();
            Assert.assertEquals(Long.valueOf(100L), status.get("percent_complete"));

            // Add some data and query
            g.addV("session").property("active", "true").property("user", "alice").iterate();
            g.addV("session").property("active", "true").property("user", "bob").iterate();
            g.addV("session").property("active", "false").property("user", "charlie").iterate();

            List<Vertex> results = g.V().hasLabel("session").has("active", "true").toList();
            Assert.assertEquals(2, results.size());

            // Drop the index
            g.call("aerospike.graph.admin.compound-index.drop")
                    .with("index_name", indexName)
                    .next();

            waitForExpressionIndexesCleared(graph);

            // Verify it's gone
            indexList = (List<String>) g.call("aerospike.graph.admin.compound-index.list").next();
            Assert.assertEquals(0, indexList.size());
        }
    }

    @Test
    public void testCompoundIndexServiceCreateInvalidPredicates() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();

            // Try to create with only one predicate (should fail)
            try {
                g.call("aerospike.graph.admin.compound-index.create")
                        .with("predicates", List.of("~label:single"))
                        .next();
                Assert.fail("Expected exception for single predicate");
            } catch (final Exception e) {
                // Expected - sanitize should reject this
            }
        }
    }

    // ==================== Combined Range Predicate Tests (gt/gte + lt/lte on same key) ====================

    @Test
    public void testExpressionIndexWithGtAndLtOnSameKey() {
        // Index with gt(1) and lt(5) on the same key creates a range (1, 5) exclusive
        // gt(1) becomes gte(2), lt(5) becomes lte(4), so range is [2, 4]
        final String indexConfig = "~label:range,value:~gt(1),value:~lt(5)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("range").property("value", 0).property("name", "below").iterate();
            g.addV("range").property("value", 1).property("name", "lower-bound").iterate();
            g.addV("range").property("value", 2).property("name", "in-range-low").iterate();
            g.addV("range").property("value", 3).property("name", "in-range-mid").iterate();
            g.addV("range").property("value", 4).property("name", "in-range-high").iterate();
            g.addV("range").property("value", 5).property("name", "upper-bound").iterate();
            g.addV("range").property("value", 6).property("name", "above").iterate();

            // Query with gt(1) and lt(5) should match values 2, 3, 4
            List<Vertex> results = g.V().hasLabel("range").has("value", P.gt(1)).has("value", P.lt(5)).toList();
            Assert.assertEquals(3, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithGteAndLteOnSameKey() {
        // Index with gte(2) and lte(4) creates an inclusive range [2, 4]
        final String indexConfig = "~label:range,value:~gte(2),value:~lte(4)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("range").property("value", 1).property("name", "below").iterate();
            g.addV("range").property("value", 2).property("name", "lower-bound").iterate();
            g.addV("range").property("value", 3).property("name", "middle").iterate();
            g.addV("range").property("value", 4).property("name", "upper-bound").iterate();
            g.addV("range").property("value", 5).property("name", "above").iterate();

            // Query with gte(2) and lte(4) should match values 2, 3, 4
            List<Vertex> results = g.V().hasLabel("range").has("value", P.gte(2)).has("value", P.lte(4)).toList();
            Assert.assertEquals(3, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithGtAndLteOnSameKey() {
        // Index with gt(1) and lte(4) creates a half-open range (1, 4]
        // gt(1) becomes gte(2), lte(4) stays lte(4), so range is [2, 4]
        final String indexConfig = "~label:range,value:~gt(1),value:~lte(4)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("range").property("value", 1).property("name", "lower-bound-exclusive").iterate();
            g.addV("range").property("value", 2).property("name", "in-range-low").iterate();
            g.addV("range").property("value", 3).property("name", "in-range-mid").iterate();
            g.addV("range").property("value", 4).property("name", "upper-bound-inclusive").iterate();
            g.addV("range").property("value", 5).property("name", "above").iterate();

            // Query with gt(1) and lte(4) should match values 2, 3, 4
            List<Vertex> results = g.V().hasLabel("range").has("value", P.gt(1)).has("value", P.lte(4)).toList();
            Assert.assertEquals(3, results.size());
        }
    }

    @Test
    public void testExpressionIndexWithGteAndLtOnSameKey() {
        // Index with gte(2) and lt(5) creates a half-open range [2, 5)
        // gte(2) stays gte(2), lt(5) becomes lte(4), so range is [2, 4]
        final String indexConfig = "~label:range,value:~gte(2),value:~lt(5)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("range").property("value", 1).property("name", "below").iterate();
            g.addV("range").property("value", 2).property("name", "lower-bound-inclusive").iterate();
            g.addV("range").property("value", 3).property("name", "in-range-mid").iterate();
            g.addV("range").property("value", 4).property("name", "in-range-high").iterate();
            g.addV("range").property("value", 5).property("name", "upper-bound-exclusive").iterate();

            // Query with gte(2) and lt(5) should match values 2, 3, 4
            List<Vertex> results = g.V().hasLabel("range").has("value", P.gte(2)).has("value", P.lt(5)).toList();
            Assert.assertEquals(3, results.size());
        }
    }

    @Test
    public void testExpressionIndexRangeWithAdditionalPredicates() {
        // Combined range with additional equality predicates
        final String indexConfig = "~label:product,category:electronics,price:~gte(100),price:~lte(500)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("product").property("category", "electronics").property("price", 50).property("name", "Cheap").iterate();
            g.addV("product").property("category", "electronics").property("price", 100).property("name", "Budget").iterate();
            g.addV("product").property("category", "electronics").property("price", 250).property("name", "Mid").iterate();
            g.addV("product").property("category", "electronics").property("price", 500).property("name", "Premium").iterate();
            g.addV("product").property("category", "electronics").property("price", 1000).property("name", "Luxury").iterate();
            g.addV("product").property("category", "furniture").property("price", 250).property("name", "Table").iterate();

            // Query should match electronics with price in [100, 500]
            List<Vertex> results = g.V().hasLabel("product")
                    .has("category", "electronics")
                    .has("price", P.gte(100))
                    .has("price", P.lte(500))
                    .toList();
            Assert.assertEquals(3, results.size());
        }
    }

    @Test
    public void testExpressionIndexNarrowRange() {
        // Test a very narrow range where gt and lt are adjacent
        // gt(3) and lt(5) should match only value 4
        final String indexConfig = "~label:narrow,value:~gt(3),value:~lt(5)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("narrow").property("value", 2).property("name", "below").iterate();
            g.addV("narrow").property("value", 3).property("name", "lower-bound").iterate();
            g.addV("narrow").property("value", 4).property("name", "only-match").iterate();
            g.addV("narrow").property("value", 5).property("name", "upper-bound").iterate();
            g.addV("narrow").property("value", 6).property("name", "above").iterate();

            // Query should match only value 4
            List<Vertex> results = g.V().hasLabel("narrow").has("value", P.gt(3)).has("value", P.lt(5)).toList();
            Assert.assertEquals(1, results.size());
            Assert.assertEquals(4, results.get(0).property("value").value());
        }
    }

    @Test
    public void testExpressionIndexSingleValueRange() {
        // Test a range that matches exactly one value: gte(5) and lte(5) matches only 5
        final String indexConfig = "~label:single,value:~gte(5),value:~lte(5)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("single").property("value", 4).property("name", "below").iterate();
            g.addV("single").property("value", 5).property("name", "exact").iterate();
            g.addV("single").property("value", 6).property("name", "above").iterate();

            // Query should match only value 5
            List<Vertex> results = g.V().hasLabel("single").has("value", P.gte(5)).has("value", P.lte(5)).toList();
            Assert.assertEquals(1, results.size());
            Assert.assertEquals(5, results.get(0).property("value").value());
        }
    }

    // ==================== Off-by-one Matching Tests (Index/Query gt/gte and lt/lte combinations) ====================

    @Test
    public void testRangeIndexGtLtQueryGteAdjustedLteAdjusted() {
        // Index: gt(1), lt(5) stored as gte(2), lte(4)
        // Query: gte(2), lte(4) - should match exactly
        final String indexConfig = "~label:item,value:~gt(1),value:~lt(5)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("item").property("value", 2).property("name", "A").iterate();
            g.addV("item").property("value", 3).property("name", "B").iterate();
            g.addV("item").property("value", 4).property("name", "C").iterate();

            // Query with equivalent gte/lte should use the index
            List<Vertex> results = g.V().hasLabel("item").has("value", P.gte(2)).has("value", P.lte(4)).toList();
            Assert.assertEquals(3, results.size());
        }
    }

    @Test
    public void testRangeIndexGteLteQueryGtAdjustedLtAdjusted() {
        // Index: gte(2), lte(4) stored as is
        // Query: gt(1), lt(5) converts to gte(2), lte(4) - should match
        final String indexConfig = "~label:item,value:~gte(2),value:~lte(4)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("item").property("value", 2).property("name", "A").iterate();
            g.addV("item").property("value", 3).property("name", "B").iterate();
            g.addV("item").property("value", 4).property("name", "C").iterate();

            // Query with gt/lt that converts to equivalent gte/lte should use the index
            List<Vertex> results = g.V().hasLabel("item").has("value", P.gt(1)).has("value", P.lt(5)).toList();
            Assert.assertEquals(3, results.size());
        }
    }

    @Test
    public void testRangeIndexGtLteQueryGteAdjustedLte() {
        // Index: gt(1), lte(4) stored as gte(2), lte(4)
        // Query: gte(2), lte(4) - should match exactly
        final String indexConfig = "~label:item,value:~gt(1),value:~lte(4)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("item").property("value", 2).property("name", "A").iterate();
            g.addV("item").property("value", 3).property("name", "B").iterate();
            g.addV("item").property("value", 4).property("name", "C").iterate();

            List<Vertex> results = g.V().hasLabel("item").has("value", P.gte(2)).has("value", P.lte(4)).toList();
            Assert.assertEquals(3, results.size());
        }
    }

    @Test
    public void testRangeIndexGteLtQueryGteLtAdjusted() {
        // Index: gte(2), lt(5) stored as gte(2), lte(4)
        // Query: gte(2), lte(4) - should match exactly
        final String indexConfig = "~label:item,value:~gte(2),value:~lt(5)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("item").property("value", 2).property("name", "A").iterate();
            g.addV("item").property("value", 3).property("name", "B").iterate();
            g.addV("item").property("value", 4).property("name", "C").iterate();

            List<Vertex> results = g.V().hasLabel("item").has("value", P.gte(2)).has("value", P.lte(4)).toList();
            Assert.assertEquals(3, results.size());
        }
    }

    @Test
    public void testRangeIndexMixedQueryMixed() {
        // Index: gt(0), lte(10) stored as gte(1), lte(10)
        // Query: gte(1), lt(11) converts to gte(1), lte(10) - should match
        final String indexConfig = "~label:item,value:~gt(0),value:~lte(10)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("item").property("value", 1).property("name", "A").iterate();
            g.addV("item").property("value", 5).property("name", "B").iterate();
            g.addV("item").property("value", 10).property("name", "C").iterate();

            // Query: gte(1), lt(11) converts to gte(1), lte(10)
            List<Vertex> results = g.V().hasLabel("item").has("value", P.gte(1)).has("value", P.lt(11)).toList();
            Assert.assertEquals(3, results.size());
        }
    }

    @Test
    public void testRangeIndexBoundaryValues() {
        // Test boundary value behavior with gt(10) and lt(20)
        // Internally stored as gte(11) and lte(19)
        final String indexConfig = "~label:boundary,value:~gt(10),value:~lt(20)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("boundary").property("value", 10).property("name", "at-lower").iterate();
            g.addV("boundary").property("value", 11).property("name", "just-above-lower").iterate();
            g.addV("boundary").property("value", 15).property("name", "middle").iterate();
            g.addV("boundary").property("value", 19).property("name", "just-below-upper").iterate();
            g.addV("boundary").property("value", 20).property("name", "at-upper").iterate();

            // Query with gt(10) and lt(20) - should NOT include 10 or 20
            List<Vertex> results = g.V().hasLabel("boundary").has("value", P.gt(10)).has("value", P.lt(20)).toList();
            Assert.assertEquals(3, results.size());

            // Verify boundary values are excluded
            for (Vertex v : results) {
                int value = (int) v.property("value").value();
                Assert.assertTrue("Value should be > 10", value > 10);
                Assert.assertTrue("Value should be < 20", value < 20);
            }
        }
    }

    @Test
    public void testRangeIndexQueryWithDifferentBounds() {
        // Index: gte(10), lte(100)
        // Query: gte(10), lte(50) - should work but only return values in [10, 50]
        // Note: The index will be used but post-filter will apply
        final String indexConfig = "~label:item,value:~gte(10),value:~lte(100)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("item").property("value", 10).property("name", "A").iterate();
            g.addV("item").property("value", 25).property("name", "B").iterate();
            g.addV("item").property("value", 50).property("name", "C").iterate();
            g.addV("item").property("value", 75).property("name", "D").iterate();
            g.addV("item").property("value", 100).property("name", "E").iterate();

            // Query for the full index range
            List<Vertex> allResults = g.V().hasLabel("item").has("value", P.gte(10)).has("value", P.lte(100)).toList();
            Assert.assertEquals(5, allResults.size());
        }
    }

    @Test
    public void testRangeIndexWithNegativeValues() {
        // Test range with negative values: gt(-5) and lt(5)
        // Stored as gte(-4) and lte(4)
        final String indexConfig = "~label:neg,value:~gt(-5),value:~lt(5)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("neg").property("value", -6).property("name", "below").iterate();
            g.addV("neg").property("value", -5).property("name", "lower-bound").iterate();
            g.addV("neg").property("value", -4).property("name", "in-range-low").iterate();
            g.addV("neg").property("value", 0).property("name", "zero").iterate();
            g.addV("neg").property("value", 4).property("name", "in-range-high").iterate();
            g.addV("neg").property("value", 5).property("name", "upper-bound").iterate();
            g.addV("neg").property("value", 6).property("name", "above").iterate();

            // Query should match values -4, 0, 4
            List<Vertex> results = g.V().hasLabel("neg").has("value", P.gt(-5)).has("value", P.lt(5)).toList();
            Assert.assertEquals(3, results.size());
        }
    }

    @Test
    public void testRangeIndexCrossingZero() {
        // Test range crossing zero: gte(-2) and lte(2)
        final String indexConfig = "~label:zero,value:~gte(-2),value:~lte(2)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("zero").property("value", -3).property("name", "below").iterate();
            g.addV("zero").property("value", -2).property("name", "lower").iterate();
            g.addV("zero").property("value", -1).property("name", "neg").iterate();
            g.addV("zero").property("value", 0).property("name", "zero").iterate();
            g.addV("zero").property("value", 1).property("name", "pos").iterate();
            g.addV("zero").property("value", 2).property("name", "upper").iterate();
            g.addV("zero").property("value", 3).property("name", "above").iterate();

            // Query should match values -2, -1, 0, 1, 2
            List<Vertex> results = g.V().hasLabel("zero").has("value", P.gte(-2)).has("value", P.lte(2)).toList();
            Assert.assertEquals(5, results.size());
        }
    }

    @Test
    public void testRangeIndexWithLongValues() {
        // Test range with Long values
        final String indexConfig = "~label:longrange,value:~gte(1000000000),value:~lte(5000000000)";
        config.setProperty(EXPRESSION_INDEX_KEY, indexConfig);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            waitForExpressionIndex(graph, indexConfig);
            final GraphTraversalSource g = graph.traversal();

            g.addV("longrange").property("value", 500000000L).property("name", "below").iterate();
            g.addV("longrange").property("value", 1000000000L).property("name", "lower").iterate();
            g.addV("longrange").property("value", 2500000000L).property("name", "middle").iterate();
            g.addV("longrange").property("value", 5000000000L).property("name", "upper").iterate();
            g.addV("longrange").property("value", 6000000000L).property("name", "above").iterate();

            // Query should match 3 values
            List<Vertex> results = g.V().hasLabel("longrange")
                    .has("value", P.gte(1000000000L))
                    .has("value", P.lte(5000000000L))
                    .toList();
            Assert.assertEquals(3, results.size());
        }
    }

    private static void waitForExpressionIndexesCleared(final FireflyGraph graph) {
        final long startTime = System.currentTimeMillis();
        List<String> indexes = graph.fireflyIndexMetadata.getExpressionIndexNames();
        while (!indexes.isEmpty()) {
            if (System.currentTimeMillis() > startTime + 30000) {
                Assert.fail("Timed out waiting for expression indexes to be cleared");
            }
            try {
                Thread.sleep(100);
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
            graph.fireflyIndexMetadata.updateMetadata();
            indexes = graph.fireflyIndexMetadata.getExpressionIndexNames();
        }
    }
}
