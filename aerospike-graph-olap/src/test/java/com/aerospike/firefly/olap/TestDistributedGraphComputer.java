package com.aerospike.firefly.olap;

import com.aerospike.firefly.io.FireflyIndexMetadata;
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
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.MessagePassingReductionStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SeedStrategy;
import org.apache.tinkerpop.gremlin.structure.Column;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.junit.Assert.assertEquals;

public class TestDistributedGraphComputer {
    private Configuration config;

    @Before
    public void beforeEach() {
        config = ConfigurationHelper.loadFromFile(Tokens.INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            //graph.getBaseGraph().dropGraphIndices(graph);
        }
    }


    private static final int INSERT_COUNT = 200000;

    public static void insertPersons(final GraphTraversalSource g) {
        for (int i = 0; i < INSERT_COUNT; i++) {
            final Vertex v1 = g.addV("Person").property("name", "person" + i).next();
            final Vertex v2 = g.addV("Person").property("name", "person" + (i + 1)).next();
            g.addE("knows").from(v1).to(v2).property("weight", i).next();
        }
    }

    public static void loadGraph(final GraphTraversalSource g) throws InterruptedException {
        final ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
        for (int i = 0; i < Runtime.getRuntime().availableProcessors(); i++) {
            executorService.submit(() -> insertPersons(g));
        }
        executorService.shutdown();
        executorService.awaitTermination(10, TimeUnit.MINUTES);
    }

    @Test
    public void testLargerInfo() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final GraphTraversalSource g = graph.traversal();
            for (int i = 0; i < 1_000_000; i++) {
            }
        }
    }

    @Test
    public void testSparkCluster2() throws Exception {
        final Instant instant = Instant.now();
        GraphTraversalSource g = null;
        try {
            g = traversal().withRemote(DriverRemoteConnection.using("35.202.200.21", 8182, "g"));
            //g.withComputer().V().hasLabel("asdf").toList();
            //System.out.println("Result: " + g.
            //        withComputer().
            //        with("evaluationTimeout", 24 * 3600 * 1000).
            //        V().hasLabel("Person").groupCount().by(__.out("HasCat").count()).
            //        toList());
            Vertex v = g.V().hasLabel("Person").limit(1).next();
            System.out.println("cnt : " + g.with("evaluationTimeout", 24 * 3600 * 1000).V().hasLabel("Person").count().next());
            System.out.println("Vid: " + v.id());
            System.out.println("Result: " + g.
                    withComputer().
                    with("evaluationTimeout", 24 * 3600 * 1000).
                    V(v.id()).out().
                    toList());
        } catch (Exception e ){
            System.out.println("Failed " + e);
            if (g != null) {
                g.close();
            }
        }
        System.out.println("Total time: " + (Instant.now().toEpochMilli() - instant.toEpochMilli()) + " ms.");

    }

    @Test
    public void testSparkCluster() throws Exception {
        final Instant instant = Instant.now();
        GraphTraversalSource g = null;
        try {
            g = traversal().withRemote(DriverRemoteConnection.using("35.202.200.21", 8182, "g"));
            //g.withComputer().V().hasLabel("asdf").toList();
            //System.out.println("Result: " + g.
            //        withComputer().
            //        with("evaluationTimeout", 24 * 3600 * 1000).
            //        V().hasLabel("Person").groupCount().by(__.out("HasCat").count()).
            //        toList());
            System.out.println("Result: " + g.
                    withComputer().
                    with("evaluationTimeout", 24 * 3600 * 1000).
                    V().hasLabel("Person").groupCount().by(__.out("HasCat").count()).
                    toList());
        } catch (Exception e) {
            System.out.println("Failed " + e);
            if (g != null) {
                g.close();
            }
        }
        System.out.println("Total time: " + (Instant.now().toEpochMilli() - instant.toEpochMilli()) + " ms.");

    }

    @Test
    public void testlocalasdf() throws Exception {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            final GraphTraversalSource g = graph.traversal();
            System.out.println("Result: " + g.
                    withComputer().
                    V().hasLabel("person").out("knows").groupCount().by("age").toList());
        }

    }

    @Test
    public void testSparkCluster3() throws Exception {
        final Instant instant = Instant.now();
        GraphTraversalSource g = null;
        try {
            g = traversal().withRemote(DriverRemoteConnection.using("35.202.200.21", 8182, "g"));
            //g.withComputer().V().hasLabel("asdf").toList();
            //System.out.println("Result: " + g.
            //        withComputer().
            //        with("evaluationTimeout", 24 * 3600 * 1000).
            //        V().hasLabel("Person").groupCount().by(__.out("HasCat").count()).
            //        toList());
            System.out.println("Result: " + g.
                    withComputer().
                    with("evaluationTimeout", 24 * 3600 * 1000).
                    V().hasLabel("Person").out("LikesCoffee").groupCount().by("country").toList());
        } catch (Exception e) {
            System.out.println("Failed " + e);
            if (g != null) {
                g.close();
            }
        }
        System.out.println("Total time: " + (Instant.now().toEpochMilli() - instant.toEpochMilli()) + " ms.");

    }

    @Test
    public void groupCount() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            List output = graph.traversal()
                    .withComputer()
                    .with("hello", "there")
                    .V()
                    .toList();

            graph.traversal().withComputer().E().properties("weight").as("a").select("a").by(T.key).toList();

            graph.traversal().withComputer().V().properties("age").as("a").select("a").by(T.key).toList();

            //System.out.println(output);
        }
    }

    @Test
    public void sample() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            List output = graph.traversal().withComputer()
                    .withStrategies(new SeedStrategy(999999))
                    .V().group().by(T.label).by(__.bothE().values("weight").order().sample(2).fold()).unfold()
                    .toList();

            System.out.println(output);
            Assert.assertEquals(2L, output.size());
        }
    }


    @Test
    public void properties() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            List output = graph.traversal().withComputer()
                    .E().properties().order().value()
                    .toList();

            System.out.println(output);
            Assert.assertEquals(6L, output.size());
        }
    }

    @Test
    public void path() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            List output = graph.traversal().withComputer()
                    .V().outE().as("e")
                    .inV().as("v")
                    .select("e").order().by("weight", Order.asc)
                    .select("v").values("name")
                    .dedup()
                    .toList();

            System.out.println(output);
            Assert.assertEquals(4L, output.size());
        }
    }

    @Test
    public void edge() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            List output = graph.traversal().withComputer()
                    .E().hasLabel("knows")
                    .toList();

            System.out.println(output);
            Assert.assertEquals(2L, output.size());
        }
    }

    @Test
    public void pathSerializationError() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            List output = graph.traversal().withComputer()
                    .V()
                    //.as("a").map(__.select("a"))
                    .as("a").label().select("a")
                    .toList();

            System.out.println(output);
            Assert.assertEquals(6L, output.size());
        }
    }

    @Ignore
    @Test
    public void stackOverflow() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            List output = graph.traversal().withComputer()
                    .V()
                    .match(__.as("a").out().as("b"))
                    .toList();

            System.out.println(output);
            Assert.assertEquals(6L, output.size());
        }
    }

    @Test
    public void testProperties() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            List output = graph.traversal().withComputer()
                    .V().both().properties().dedup().count().toList();

            System.out.println(output);
        }
    }

    Edge getEdge(final GraphTraversalSource g, final String outVertexName, final String inVertexName, final String edgeLabel) {
        return g.V().has("name", outVertexName).outE(edgeLabel).as("e").inV().has("name", inVertexName).<Edge>select("e").toList().get(0);
    }

    @Test
    public void testE11outVoutE() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            final Edge e10 = getEdge(graph.traversal(), "josh", "ripple", "created");
            final Edge e11 = getEdge(graph.traversal(), "josh", "lop", "created");

            Object output = graph.traversal().withComputer().E(e11.id()).outV().outE().has(T.id, e10.id()).next();

            System.out.println(output);
        }
    }

    @Test
    public void testKnows() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            List output = graph.traversal().withComputer().E().hasLabel("knows").toList();

            System.out.println(output);
        }
    }

    @Ignore
    @Test
    public void failures() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            List output = graph.traversal().withComputer()
                    .V().repeat(__.repeat(out("created")).until(__.has("name", "ripple"))).emit().values("lang").toList();
            System.out.println(output);
            //Long count = graph.traversal().V().bothE().properties().dedup().count().next();
            //System.out.println(count);
        }
    }

    @Ignore
    @Test
    public void testF() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            graph.traversal().withComputer().V().repeat(__.both()).times(10).as("a").out().as("b").select("a", "b").count().next();
            //Long count = graph.traversal().V().bothE().properties().dedup().count().next();
            //System.out.println(count);
        }
    }

    @Test
    public void testHasIdEmpty() {
        // expected 6L
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            Long count1 = graph.traversal().V().hasId(Collections.emptyList()).count().next();
            Long count = graph.traversal().withComputer().V().hasId(Collections.emptyList()).count().next();
            System.out.println(count1);
            System.out.println(count);
        }
    }

    @Test
    public void tesasdftAge() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            List<Vertex> v1 = graph.traversal().V().has("age", P.gt(18).and(P.lt(30)).or(P.gt(35))).toList();
            List<Vertex> v2 = graph.traversal().withComputer().V(1, 2, 3, 4, 5, "1").toList();
            System.out.println(v1);
            System.out.println(v2);
        }
    }

    @Test
    public void testAge() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            List<Vertex> v1 = graph.traversal().V().has("age", P.gt(18).and(P.lt(30)).or(P.gt(35))).toList();
            List<Vertex> v2 = graph.traversal().withComputer().V().has("age", P.gt(18).and(P.lt(30)).or(P.gt(35))).toList();
            System.out.println(v1);
            System.out.println(v2);
        }
    }

    @Test
    public void testEdges() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            List<Edge> v1 = graph.traversal().E().toList();
            List<Edge> v2 = graph.traversal().withComputer().E().toList();
            System.out.println(v1);
            System.out.println(v2);
        }
    }

    @Ignore
    @Test
    public void testUnion() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            List output2 = graph.traversal()
                    .union().toList();

            System.out.println(output2);

            List output = graph.traversal().withComputer()
                    .union().toList();
            System.out.println(output);
        }
    }

    @Test
    public void testSelect() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            List output2 = graph.traversal().V().as("a").label().select("a").toList();

            List output = graph.traversal().withComputer().V().as("a").label().select("a").toList();
            System.out.println("outputOLAP : " + output);
            System.out.println("outputOLTP : " + output2);
        }
    }

    @Test
    public void testNullid() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            List<Vertex> output = graph.traversal()
                    .V(1, null)
                    .out()
                    .toList();
            System.out.println(output);

            List<Vertex> output2 = graph.traversal().withComputer()
                    .V(1, null)
                    .out()
                    .toList();

            System.out.println(output2);
        }
    }

    @Test
    public void testOutLimit() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            Object id = graph.traversal().V().has("name", "marko").id().next();
            List<Vertex> output = graph.traversal().withComputer().V(id).out().limit(2).toList();
            Assert.assertEquals(2L, output.size());
            System.out.println(output);
        }
    }

    @Ignore
    @Test
    public void testIndex() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            List<?> output = graph.traversal().withComputer()
                    .V().hasLabel("software").index().unfold().toList();
            System.out.println(output);
        }
    }

    @Test
    public void testOrderoutE() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            final List<?> output = graph.traversal().withComputer().V().outE().order().by("weight", Order.desc).values("weight").toList();
            Assert.assertEquals(6, output.size());
        }
    }

    @Test
    public void testPI() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            List<Vertex> output = graph.traversal().withComputer()
                    .V(1)
                    .out()
                    .toList();

            List<Vertex> output2 = graph.traversal()
                    .V(1)
                    .out()
                    .toList();

            System.out.println(output);
            System.out.println(output2);
        }
    }

    @Test
    public void testRepeatTimes() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            List<Vertex> output = graph.traversal().withComputer()
                    .V(1)
                    .repeat(__.out())
                    .times(2)
                    .toList();

            System.out.println(output);
        }
    }

    @Test
    public void testEKnows() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            List<Edge> output = graph.traversal().withComputer()
                    .E().hasLabel("knows")
                    .toList();

            System.out.println(output);
        }
    }

    @Test
    public void testE() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            List<Edge> output = graph.traversal().withComputer()
                    .E()
                    .toList();

            System.out.println(output);
        }
    }

    @Ignore
    @Test
    public void testRepeatAsSelect() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);

            List<?> output = graph.traversal().withComputer().
                    V().repeat(__.both()).times(10).as("a").out().as("b").select("a", "b").toList();

            System.out.println(output);
        }
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
            List<?> output = graph.traversal().withComputer().V().has("name", "marko").repeat(__.out()).until(__.hasLabel("person")).toList();
            System.out.println("expected: " + graph.traversal().V().has("name", "marko").repeat(__.out()).until(__.hasLabel("person")).toList());
            System.out.println("got: " + output);
        }
    }

    @Test
    public void test_dataset_gVCount() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            final long count = graph.traversal().V().count().next();
            final long olapCount = graph.traversal().withComputer().V().count().next();
            Assert.assertEquals(count, olapCount);
        }
    }

    @Test
    public void test_dataset_gVGroupCountByOutCount() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            final Map<Object, Long> oltp = graph.traversal().V().groupCount().by(__.out("knows").count()).next();
            final Map<Object, Long> olap = graph.traversal().withComputer().V().groupCount().by(__.out("knows").count()).next();
            System.out.println("OLTP: " + oltp);
            System.out.println("OLAP: " + olap);
            Assert.assertTrue(oltp.keySet().containsAll(olap.keySet()));
            Assert.assertEquals(oltp.keySet().size(), olap.keySet().size());
            oltp.keySet().forEach(k -> Assert.assertEquals(oltp.get(k), olap.get(k)));
        }
    }

    @Test
    public void test_dataset_gVGroupCountByOutKnowsCount() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            final Map<Object, Long> oltp = graph.traversal().V().groupCount().by(__.out("knows").count()).next();
            final Map<Object, Long> olap = graph.traversal().withComputer().V().groupCount().by(__.out("knows").count()).next();
            System.out.println("OLTP: " + oltp);
            System.out.println("OLAP: " + olap);
            Assert.assertTrue(oltp.keySet().containsAll(olap.keySet()));
            Assert.assertEquals(oltp.keySet().size(), olap.keySet().size());
            oltp.keySet().forEach(k -> Assert.assertEquals(oltp.get(k), olap.get(k)));
        }
    }

    @Test
    public void test_VHasLabelGroupCountByOutKnowsCount() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            final Map<Object, Long> oltp = graph.traversal().V().hasLabel("person").groupCount().by(__.out("knows").count()).next();
            final Map<Object, Long> olap = graph.traversal().withComputer().V().hasLabel("person").groupCount().by(__.out("knows").count()).next();
            System.out.println("OLTP: " + oltp);
            System.out.println("OLAP: " + olap);
            Assert.assertTrue(oltp.keySet().containsAll(olap.keySet()));
            Assert.assertEquals(oltp.keySet().size(), olap.keySet().size());
            oltp.keySet().forEach(k -> Assert.assertEquals(oltp.get(k), olap.get(k)));
        }
    }

    @Test
    public void test_VHasLabelCount() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            final Long oltp = graph.traversal().V().hasLabel("person").count().next();
            final Long olap = graph.traversal().withComputer().V().hasLabel("person").count().next();
            System.out.println("OLTP: " + oltp);
            System.out.println("OLAP: " + olap);
            Assert.assertEquals(oltp, olap);
        }
    }

    @Test
    public void test_VHasLabelGroupCountBy() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            System.out.println(graph.traversal().V().has("name", "marko").elementMap().toList());
            final Map<Object, Long> oltp = graph.traversal().V().hasLabel("person").groupCount().by("name").next();
            final Map<Object, Long> olap = graph.traversal().withComputer().V().hasLabel("person").groupCount().by("name").next();
            System.out.println("OLTP: " + oltp);
            System.out.println("OLAP: " + olap);
            Assert.assertTrue(oltp.keySet().containsAll(olap.keySet()));
            Assert.assertEquals(oltp.keySet().size(), olap.keySet().size());
            oltp.keySet().forEach(k -> Assert.assertEquals(oltp.get(k), olap.get(k)));
        }
    }

    @Test
    public void testVHasLabelOutGroupCountBy() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            final Map<Object, Long> oltp = graph.traversal().V().hasLabel("person").out("person").groupCount().by("name").next();
            final Map<Object, Long> olap = graph.traversal().withComputer().V().hasLabel("person").out("person").groupCount().by("name").next();
            System.out.println("OLTP: " + oltp);
            System.out.println("OLAP: " + olap);
            Assert.assertTrue(oltp.keySet().containsAll(olap.keySet()));
            Assert.assertEquals(oltp.keySet().size(), olap.keySet().size());
            oltp.keySet().forEach(k -> Assert.assertEquals(oltp.get(k), olap.get(k)));
        }
    }

    @Test
    public void testBarrier() {
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            List<?> output = graph.traversal().withComputer().V().group().by(T.label).toList();
            Assert.assertEquals(1, output.size());
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
            result = g.V().local(__.bothE("created").limit(1)).otherV().values("name").toList();
            assertEquals(5, result.size());

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

    @Ignore
    @Test
    public void testLabelIndex() {
        createLabelIndex();
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            List<FireflyIndexMetadata.IndexInfo> indexes = graph.fireflyIndexMetadata.getPropertyIndexInfos();
            Assert.assertEquals(1, indexes.size());
            Assert.assertEquals(graph.getBaseGraph().V_LABEL_INDEX_NAME, indexes.get(0).indexName);
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            List<Vertex> output = graph.traversal().withComputer().V().hasLabel("person").out().out().toList();
            Assert.assertEquals(2, output.size());
            System.out.println(output);
        }
    }

    void createLabelIndex() {
        config.setProperty("aerospike.graph.index.vertex.label.enabled", "true");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.fireflyIndexMetadata.updateMetadata();
            List<FireflyIndexMetadata.IndexInfo> indexes = graph.fireflyIndexMetadata.getPropertyIndexInfos();
            while (indexes.size() == 0) {
                try {
                    graph.fireflyIndexMetadata.updateMetadata();
                    indexes = graph.fireflyIndexMetadata.getPropertyIndexInfos();
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                }
            }
            Assert.assertEquals(1, indexes.size());
            Assert.assertEquals(graph.getBaseGraph().V_LABEL_INDEX_NAME, indexes.get(0).indexName);
        }
    }

    void createIndex(final String propertyName) {
        config.setProperty("aerospike.graph.index.vertex.properties", propertyName);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            // 1 second to create index.
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
            }
            graph.fireflyIndexMetadata.updateMetadata();
            List<FireflyIndexMetadata.IndexInfo> indexes = graph.fireflyIndexMetadata.getPropertyIndexInfos();
            Assert.assertEquals(2, indexes.size());
            Assert.assertEquals(propertyName, indexes.get(0).key);
            Assert.assertEquals(propertyName, indexes.get(1).key);
        }
    }

    @Test
    public void testPropertyIndex() {
        createIndex("name");
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            graph.traversal().V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            List<Vertex> output = graph.traversal().withComputer().V().has("name", "marko").out().toList();
            Assert.assertEquals(3, output.size());
            System.out.println(output);
        }
    }
}