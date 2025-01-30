package com.aerospike.firefly.olap;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class TestDistributedGraphComputer {
    private Configuration config;

    @Before
    public void beforeEach() {
        config = ConfigurationHelper.loadFromFile(Tokens.INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");
    }

    @Test
    public void testBasic() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            List<Vertex> output = graph.traversal().withComputer().V().has("name", "marko").out().toList();
            Assert.assertEquals(3, output.size());
            System.out.println(output);
        }
    }

    @Test
    public void testSideEffect() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            System.out.println(graph.traversal().V().store("a").by("name").out().cap("a").toList());
            List<?> output = graph.traversal().withComputer().V().store("a").by("name").out().cap("a").toList();
            System.out.println(output);
            Assert.assertTrue(output.size() == 1 && output.get(0) instanceof BulkSet);
            final BulkSet result = (BulkSet) output.get(0);
            assertEquals(6, result.size());
            Assert.assertTrue(result.asBulk().keySet().containsAll(Set.of("marko", "vadas", "josh", "peter", "ripple", "lop")));
            result.asBulk().values().forEach(v -> Assert.assertEquals(1L, v));
        }
    }

    @Test
    public void testElementMap() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            System.out.println(graph.traversal().V().has("name", "marko").elementMap().toList());
            List<?> output = graph.traversal().withComputer().V().has("name", "marko").elementMap().toList();
            System.out.println(output);
        }
    }

    @Test
    public void testRepeat() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            System.out.println(graph.traversal().V().has("name", "marko").repeat(__.out()).until(__.hasLabel("person")).toList());
            List<?> output = graph.traversal().withComputer().V().has("name", "marko").repeat(__.out()).until(__.hasLabel("person")).toList();
            System.out.println(output);
        }
    }

    @Test
    public void testBarrier() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            List<?> output = graph.traversal().withComputer().V().group().by(T.label).toList();
            Assert.assertEquals(3, output.size());
            System.out.println(output);
        }
    }

    @Test
    public void testFairlyBasic() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            System.out.println(graph.traversal().V().hasLabel("person").out().toList());
            System.out.println("Actual: " + graph.traversal().V().hasLabel("person").out().out().toList());
            List<Vertex> output = graph.traversal().withComputer().V().hasLabel("person").out().out().toList();
            Assert.assertEquals(2, output.size());
            System.out.println(output);
        }
    }

    @Test
    public void testPath() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            System.out.println(graph.traversal().V().hasLabel("person").out().toList());
            System.out.println("Actual: " + graph.traversal().V().hasLabel("person").out().out().path().toList());
            List<Path> output = graph.traversal().withComputer().V().hasLabel("person").out().out().path().toList();
            Assert.assertEquals(2, output.size());
            System.out.println(output);
        }
    }

    @Test
    @Ignore
    public void testFairlyBasicEdge() {
        // to get firefly elements
        System.setProperty("is.testing", "true");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            System.out.println(graph.traversal().V().hasLabel("person").out().toList());
            System.out.println("Actual: " + graph.traversal().V().hasLabel("person").out().out().toList());
            List<Edge> output = graph.traversal().withComputer().V().hasLabel("person").out().outE().toList();
            Assert.assertEquals(2, output.size());
            System.out.println(output);
        }
    }

    @Test
    public void testOtherV() {
        // System.setProperty("is.testing", "true");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            final GraphTraversalSource g = graph.traversal().withComputer();

            // filter out everything
            List result = g.V().outE().not(__.hasLabel("knows")).otherV().hasLabel("test").toList();
            assertEquals(0, result.size());

            // only part of results is valid
            result = g.V().outE().not(__.hasLabel("knows")).otherV().has("name", "ripple").toList();
            assertEquals(1, result.size());

            // no hasContainer
            result = g.V().outE().not(__.hasLabel("knows")).otherV().toList();
            assertEquals(4, result.size());

            // borrowed from TinkerPop Feature tests
//            result = g.V().local(__.bothE("created").limit(1)).otherV().values("name").toList();
//            assertEquals(5, result.size()); // return 20 now, 5 correct with bulk 4

            result = g.V(4).bothE().otherV().toList();
            assertEquals(3, result.size());

            result = g.V(4).bothE().has("weight", P.lt(1.0)).otherV().toList();
            assertEquals(1, result.size());
            assertEquals(3, ((Vertex) result.get(0)).id());
        }
    }

    @Test
    public void testConfig() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            System.out.println("Config: " + AerospikeConnection.InfoOps.getMaxParallelSindexes(graph.getBaseGraph(), graph.getBaseGraph().namespace));
        }

    }

    @Test
    public void testMap() {
        SparkSession spark = SparkSession.builder()
                .appName("Java Map Schema Example")
                .master("local[*]")
                .getOrCreate();

        // Define schema
        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("label", DataTypes.StringType, false)
                .add("properties", DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType, true));

        // Create data
        Map<String, String> properties = new HashMap<>();
        properties.put("key1", "value1");
        properties.put("key2", "value2");

        Row row = RowFactory.create("vertexId", "vertexLabel", properties);

        // Create DataFrame
        Dataset<Row> df = spark.createDataFrame(java.util.Collections.singletonList(row), schema);

        // Show DataFrame
        df.show(false);

        spark.stop();
    }
}
