package com.aerospike.firefly;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.driver.Cluster;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.Merge;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.util.CollectionUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.apache.tinkerpop.gremlin.AbstractGremlinTest.verifyRootIdentification;
import static org.apache.tinkerpop.gremlin.AbstractGremlinTest.verifyUniqueStepIds;
import static org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest.checkMap;
import static org.apache.tinkerpop.gremlin.process.AbstractGremlinProcessTest.checkSideEffects;
import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;
import static org.apache.tinkerpop.gremlin.process.traversal.P.gt;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.both;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.count;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.repeat;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;

@Ignore
public class OLAPTesting {

    @Test
    public void testConn() {
        final Cluster cluster = Cluster.build()
                .addContactPoint("34.27.68.133")
                .port(8182)
                .create();
        final DriverRemoteConnection connection = DriverRemoteConnection.using(cluster);
        final GraphTraversalSource g = traversal().withRemote(connection);
        g.inject(0).iterate();
    }

    @Test
    public void testLoad() {
        final Cluster cluster = Cluster.build()
                .addContactPoint("34.27.68.133")
                .port(8182)
                .create();
        final DriverRemoteConnection connection = DriverRemoteConnection.using(cluster);
        System.out.println("dropping");
        final GraphTraversalSource g = traversal().withRemote(connection);
        g.V().drop().iterate();
        System.out.println("dropped");
        g.with("evaluationTimeout", 24 * 60 * 60 * 1000).
            call("aerospike.graphloader.admin.bulk-load.load").
            with("aerospike.graphloader.vertices", "/opt/aerospike-graph/etc/sampledata/vertices").
            with("aerospike.graphloader.edges", "/opt/aerospike-graph/etc/sampledata/edges").iterate();
        System.out.println("loaded");
    }

    @Test
    public void testPerfLoad() {
        Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        conf.setProperty("aerospike.graph.index.vertex.label", "true");
        try (FireflyGraph graph = FireflyGraph.open(conf)) {
            final GraphTraversalSource g = graph.traversal();
            g.V().drop().iterate();
            final List<Vertex> roots = new ArrayList<>();
            for (int i = 0; i < 10_000; i++) {
                roots.add(g.addV("Root").property("name", "root" + i).next());
            }
            final List<Vertex> branches = new ArrayList<>();
            for (int i = 0; i < 100_000; i++) {
                branches.add((Vertex) g.addV("Branch").
                        property("name", "branch" + i).as("a").
                        addE("HAS_BRANCH").
                        from(roots.get(i % 10_000)).select("a").next());
            }
            for (int i = 0; i < 1_000_000; i++) {
                g.addV("Leaf").
                        property("name", "leaf" + i).
                        addE("HAS_LEAF").
                        from(branches.get(i % 100_000)).next();
            }
        }
    }

    class PerfInfo {
        final String name;
        final List<Long> durations;
        PerfInfo(String name, List<Long> durations) {
            this.name = name;
            this.durations = durations;
        }

        public String toString() {
            return String.format("%s\n\tMax:\t%.2f\n\tAvg:\t%.2f\n\tMin:\t%.2f",
                    name,
                    durations.stream().mapToDouble(l -> l.floatValue() / 1000f).max().orElse(0),
                    durations.stream().mapToDouble(l -> l.floatValue() / 1000f).average().orElse(0),
                    durations.stream().mapToDouble(l -> l.floatValue() / 1000f).min().orElse(0));
        }
    }

    public PerfInfo getPerformance(final String name,
                               final int warmupIterations,
                               final int iterations,
                               final Runnable query) {
        final List<Long> durations = new ArrayList<>();
        for (int i = 0; i < warmupIterations; i++) {
            Long start = System.nanoTime();
            query.run();
            Long end = System.nanoTime();
            System.out.println("Warmup: " + i + " Duration: " + (end - start) / 1000_000);
        }
        for (int i = 0; i < iterations; i++) {
            Long start = System.nanoTime();
            query.run();
            Long end = System.nanoTime();
            System.out.println("Iteration: " + i + " Duration: " + (end - start) / 1000_000);
            durations.add((end - start) / 1000_000);
        }
        return new PerfInfo(name, durations);
    }

    @Test
    public void testQueryCount() {
        Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (FireflyGraph graph = FireflyGraph.open(conf)) {
            final GraphTraversalSource c = graph.traversal().withComputer();
            final GraphTraversalSource g = graph.traversal();
            PerfInfo gVCount = getPerformance("g.V().count()", 40, 10, () -> g.V().count().next());
            PerfInfo gCCount = getPerformance("c.V().count()", 40, 10, () -> c.V().count().next());
            System.out.println(gVCount);
            System.out.println(gCCount);
        }
    }

    @Test
    public void testQueryGroupCount() {
        Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (FireflyGraph graph = FireflyGraph.open(conf)) {
            final GraphTraversalSource c = graph.traversal().withComputer();
            final GraphTraversalSource g = graph.traversal();

            PerfInfo gQuery = getPerformance("g.V().hasLabel('Root').groupCount().by('name')", 40, 10, () -> g.V().hasLabel("Root").groupCount().by("name").next());
            PerfInfo cQuery = getPerformance("c.V().hasLabel('Root').groupCount().by('name')", 40, 10, () -> c.V().hasLabel("Root").groupCount().by("name").next());

            System.out.println(gQuery);
            System.out.println(cQuery);
        }
    }


    @Test
    public void g_VX1X_localXoutEXknowsX_limitX1XX_inV_name() {
        Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (FireflyGraph graph = FireflyGraph.open(conf)) {
            final GraphTraversalSource c = graph.traversal().withComputer();
            final GraphTraversalSource g = graph.traversal();
            g.V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            final Vertex marko = tg.traversal().V().has("name", "marko").next();
            final Object markoId = marko.id();
            final Object name = c.V(markoId).out("knows").next();
            Assert.assertTrue(name.equals("vadas") || name.equals("josh"));
        }
    }

    @Test
    public void g_V_outXcreatedX_unionXasXinternaldataset_inXcreatedX_hasXname_markoX_selectXinternaldataset__asXinternaldataset_inXcreatedX_inXknowsX_hasXname_markoX_selectXinternaldatasetX_groupCount_byXnameX() {

        Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (FireflyGraph graph = FireflyGraph.open(conf)) {
            final GraphTraversalSource c = graph.traversal().withComputer();
            final GraphTraversalSource g = graph.traversal();
            g.V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            c.V().both("knows").both("knows").toList();
            //Traversal<Vertex, Map<String, Long>> traversal = c.V().union(
            //        repeat(out()).times(2).groupCount("m").by("lang"),
            //        repeat(in()).times(2).groupCount("m").by("name")).cap("m");
            //printTraversalForm(traversal);
            //Assert.assertTrue(traversal.hasNext());
            //Map<String, Long> map = (Map) traversal.next();
            //Assert.assertFalse(traversal.hasNext());
            //Assert.assertEquals(2L, (long) map.size());
            //Assert.assertEquals(1L, (long) map.get("ripple"));
            //Assert.assertEquals(6L, (long) map.get("lop"));
        }
    }

    @Test
    public void testGrateful() {
        Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (FireflyGraph graph = FireflyGraph.open(conf)) {
            final GraphTraversalSource g = graph.traversal();
            GraphTraversalSource g1 = traversal().withRemote(DriverRemoteConnection.using("localhost", 8182));
            System.out.println(g1.V().not(__.hasLabel("Foo")).otherV().not(__.hasLabel("bar")).project("a").by(__.fold()).profile().toList());
            GraphTraversal traversal = g.V().not(__.hasLabel("Foo")).hasLabel("bar").otherV();
            System.out.println("   pre-strategy:" + traversal);
            if (!traversal.asAdmin().isLocked()) traversal.asAdmin().applyStrategies();
            System.out.println("  post-strategy:" + traversal);

            graph.traversal().V().drop().iterate();
            GraphHelper.cloneElements(TinkerFactory.createGratefulDead(), graph);
            traversal = graph.traversal().withComputer().V().repeat(both("followedBy")).times(2).<String, Long>group("a").by("songType").by(count()).cap("a");
            //graph.traversal().withComputer().V().both("followedBy").both("followedBy").toList();
            // Expected :160968
            // Actual   :154867
            printTraversalForm(traversal);
            checkMap(new HashMap<String, Long>() {
                {
                    this.put("original", 771317L);
                    this.put("", 160968L);
                    this.put("cover", 368579L);
                }
            }, (Map)traversal.next());
            Assert.assertFalse(traversal.hasNext());
            checkSideEffects(traversal.asAdmin().getSideEffects(), new Object[]{"a", HashMap.class});
        }
    }

    @Test
    public void testGrateful2() {
        Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (FireflyGraph graph = FireflyGraph.open(conf)) {
            final GraphTraversalSource c = graph.traversal().withComputer();
            final GraphTraversalSource g = graph.traversal();
            g.V().drop().iterate();
            final Graph tg = TinkerFactory.createGratefulDead();
            GraphHelper.cloneElements(tg, graph);
            // Traversal<Vertex, Map<String, Long>> traversal = c.V().repeat(both("followedBy")).times(2).<String, Long>group("a").by("songType").by(count()).cap("a");
            c.V().toList();
            //printTraversalForm(traversal);
            //checkMap(new HashMap<String, Long>() {
            //    {
            //        this.put("original", 771317L);
            //        this.put("", 160968L);
            //        this.put("cover", 368579L);
            //    }
            //}, (Map)traversal.next());
            //Assert.assertFalse(traversal.hasNext());
            //checkSideEffects(traversal.asAdmin().getSideEffects(), new Object[]{"a", HashMap.class});
        }
    }

    @Test
    public void fake() {
        Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (FireflyGraph graph = FireflyGraph.open(conf)) {
            final GraphTraversalSource c = graph.traversal().withComputer();
            final GraphTraversalSource g = graph.traversal();
            g.V().drop().iterate();
            Vertex v1 = g.addV().next();
            Vertex v2 = g.addV().next();
            Vertex v3 = g.addV().next();
            g.addE("1").from(v1).to(v2).next();
            g.addE("1").from(v1).to(v2).next();
            g.addE("1").from(v2).to(v1).next();
            g.addE("1").from(v2).to(v1).next();

            g.addE("2").from(v1).to(v3).next();
            g.addE("2").from(v1).to(v3).next();
            g.addE("2").from(v3).to(v1).next();
            g.addE("2").from(v3).to(v1).next();

            g.addE("3").from(v2).to(v3).next();
            g.addE("3").from(v2).to(v3).next();
            g.addE("3").from(v3).to(v2).next();
            g.addE("3").from(v3).to(v2).next();

            // Traversal<Vertex, Map<String, Long>> traversal = c.V().repeat(both("followedBy")).times(2).<String, Long>group("a").by("songType").by(count()).cap("a");
            System.out.println(g.withComputer().V().both().toList());
            //printTraversalForm(traversal);
            //checkMap(new HashMap<String, Long>() {
            //    {
            //        this.put("original", 771317L);
            //        this.put("", 160968L);
            //        this.put("cover", 368579L);
            //    }
            //}, (Map)traversal.next());
            //Assert.assertFalse(traversal.hasNext());
            //checkSideEffects(traversal.asAdmin().getSideEffects(), new Object[]{"a", HashMap.class});
        }
    }

    @Test
    public void g_V_both_groupCountXaX_byXlabelX_asXbX_barrier_whereXselectXaX_selectXsoftwareX_isXgtX2XXX_selectXbX_name() {
        Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (FireflyGraph graph = FireflyGraph.open(conf)) {
            final GraphTraversalSource c = graph.traversal().withComputer();
            final GraphTraversalSource g = graph.traversal();
            g.V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            Traversal<Vertex, String> traversal = c.V().both().groupCount("a").by(T.label).as("b").barrier().where(__.select("a").select("software").is(gt(2))).select("b").values("name");
            this.printTraversalForm(traversal);
            checkResults(Arrays.asList("lop", "lop", "lop", "peter", "marko", "marko", "marko", "ripple", "vadas", "josh", "josh", "josh"), traversal);
            checkSideEffects(traversal.asAdmin().getSideEffects(), new Object[]{"a", HashMap.class});
        }
    }

    @Test
    public void test_bothbothdedup() {
        Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (FireflyGraph graph = FireflyGraph.open(conf)) {
            final GraphTraversalSource c = graph.traversal().withComputer();
            final GraphTraversalSource g = graph.traversal();
            g.V().drop().iterate();
            final Graph tg = TinkerFactory.createModern();
            GraphHelper.cloneElements(tg, graph);
            //System.out.println("OLAP: " + c.V().both().both().dedup().toList());
            //System.out.println("OLTP: " + g.V().both().both().dedup().toList());
            // Expected results: [v[1], v[2], v[4], v[6], v[3], v[5]]
            // Actual results:   [v[1], v[3], v[5]]
            Traversal<Vertex, Vertex> traversal = c.V().both().both().dedup();
            printTraversalForm(traversal);
            checkResults(
                    Arrays.asList(
                            convertToVertex(c, "marko"),
                            convertToVertex(c, "vadas"),
                            convertToVertex(c, "josh"),
                            convertToVertex(c, "peter"),
                            convertToVertex(c, "lop"),
                            convertToVertex(c, "ripple")),
                    traversal);
        }
    }

    public void printTraversalForm(final Traversal traversal) {
        System.out.println("   pre-strategy:" + traversal);
        if (!traversal.asAdmin().isLocked()) traversal.asAdmin().applyStrategies();
        System.out.println("  post-strategy:" + traversal);
        verifyUniqueStepIds(traversal.asAdmin());
        verifyRootIdentification(traversal.asAdmin(), true);
    }

    public Vertex convertToVertex(final GraphTraversalSource g, final String vertexName) {
        // all test graphs have "name" as a unique id which makes it easy to hardcode this...works for now
        return g.V().has("name", vertexName).toList().get(0);
    }

    public static <T> void checkResults(final List<T> expectedResults, final Traversal<?, T> traversal) {
        final List<T> results = traversal.toList();
        assertThat(traversal.hasNext(), is(false));
        if (expectedResults.size() != results.size()) {
            System.out.println("Expected results: " + expectedResults);
            System.out.println("Actual results:   " + results);
            assertEquals("Checking result size", expectedResults.size(), results.size());
        }

        for (T t : results) {
            if (t instanceof Map) {
                assertThat("Checking map result existence: " + t, expectedResults.stream().filter(e -> e instanceof Map).anyMatch(e -> internalCheckMap((Map) e, (Map) t)), is(true));
            } else if (t instanceof List) {
                assertThat("Checking list result existence: " + t, expectedResults.stream().filter(e -> e instanceof List).anyMatch(e -> internalCheckList((List) e, (List) t)), is(true));
            } else {
                assertThat("Checking result existence: " + t, expectedResults.contains(t), is(true));
            }
        }
        final Map<T, Long> expectedResultsCount = new HashMap<>();
        final Map<T, Long> resultsCount = new HashMap<>();
        expectedResults.forEach(t -> MapHelper.incr(expectedResultsCount, t, 1L));
        results.forEach(t -> MapHelper.incr(resultsCount, t, 1L));
        assertEquals("Checking indexing is equivalent", expectedResultsCount.size(), resultsCount.size());
        expectedResultsCount.forEach((k, v) -> assertEquals("Checking result group counts", v, resultsCount.get(k)));
    }

    private static <A> boolean internalCheckList(final List<A> expectedList, final List<A> actualList) {
        if (expectedList.size() != actualList.size()) {
            return false;
        }
        for (int i = 0; i < actualList.size(); i++) {
            if (!actualList.get(i).equals(expectedList.get(i))) {
                return false;
            }
        }
        return true;
    }

    private static <A, B> boolean internalCheckMap(final Map<A, B> expectedMap, final Map<A, B> actualMap) {
        final List<Map.Entry<A, B>> actualList = actualMap.entrySet().stream().sorted(Comparator.comparing(a -> a.getKey().toString())).collect(Collectors.toList());
        final List<Map.Entry<A, B>> expectedList = expectedMap.entrySet().stream().sorted(Comparator.comparing(a -> a.getKey().toString())).collect(Collectors.toList());

        if (expectedList.size() != actualList.size()) {
            return false;
        }

        for (int i = 0; i < actualList.size(); i++) {
            if (!Objects.equals(actualList.get(i).getKey(), expectedList.get(i).getKey())) {
                return false;
            }
            if (!Objects.equals(actualList.get(i).getValue(), expectedList.get(i).getValue())) {
                return false;
            }
        }
        return true;
    }

    @Test
    public void testQueryGroupCountOut() {
        Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (FireflyGraph graph = FireflyGraph.open(conf)) {
            final GraphTraversalSource c = graph.traversal().withComputer();
            final GraphTraversalSource g = graph.traversal();

            final Map<Object, Object> mergeVMap = new HashMap<>();
            mergeVMap.put(T.id, 1);
            g.mergeV(mergeVMap).property("foo", "bar").next();

           // Instant start = Instant.now();
           // PerfInfo cQuery = getPerformance("c.V().hasLabel('Root').out().groupCount().by(__.out().count())", 2, 5, () -> c.V().hasLabel("Root").out().groupCount().by(__.out().count()).next());
           // Instant end = Instant.now();
           // Instant start2 = Instant.now();
           // PerfInfo gQuery = getPerformance("g.V().hasLabel('Root').out().groupCount().by(__.out().count())", 2, 5, () -> g.V().hasLabel("Root").out().groupCount().by(__.out().count()).next());
           // Instant end2 = Instant.now();
//
           // System.out.println("OLTP:" + g.V().hasLabel("Root").out().groupCount().by(__.out().count()).toList());
           // System.out.println("OLAP: " + c.V().hasLabel("Root").out().groupCount().by(__.out().count()).toList());
//
           // System.out.println(gQuery);
           // System.out.println(cQuery);
           // System.out.println("Time: " + Duration.between(start, end).getSeconds() + "s");
           // System.out.println("Time: " + Duration.between(start2, end2).getSeconds() + "s");
        }
    }

    @Test
    public void testNoAuth() {
        final Cluster cluster = Cluster.build()
                .addContactPoint("34.27.68.133")
                .port(8182)
                .create();
        final DriverRemoteConnection connection = DriverRemoteConnection.using(cluster);
        final GraphTraversalSource g = traversal().withRemote(connection);

        // Test time per run
        List<Long> durations = executeCoffeeOrigin(g).stream().map(Duration::getSeconds).collect(Collectors.toList());
        System.out.println("Durations: " + durations);
        System.out.println("Average duration: " + durations.stream().mapToLong(Long::longValue).average().orElse(0));
        System.out.println("Max duration: " + durations.stream().mapToLong(Long::longValue).max().orElse(0));
        System.out.println("Min duration: " + durations.stream().mapToLong(Long::longValue).min().orElse(0));


        //g.with_("evaluationTimeout", 24 * 3600 * 1000).V().count().next()
        //g.with_("evaluationTimeout", 24 * 3600 * 1000).V().groupCount().by(__.out().count()).to_list()
        //g.with_("evaluationTimeout", 24 * 3600 * 1000).V().groupCount().by(__.out('HasCat').count()).next()
        //g.with_("evaluationTimeout", 24 * 3600 * 1000).V().hasLabel('Person').groupCount().by(__.out('HasCat').count()).next()
        //g.with_("evaluationTimeout", 24 * 3600 * 1000).V().hasLabel('Person').count().next()
        //g.with_("evaluationTimeout", 24 * 3600 * 1000).V().hasLabel('Person').groupCount().by('race').to_list()
        //g.with_("evaluationTimeout", 24 * 3600 * 1000).V().hasLabel('Person').out('LikesCoffee').groupCount().by('country').to_list()
        //g.with_("evaluationTimeout", 24 * 3600 * 1000).E().hasLabel('LikesCoffee').count().next()
        //g.with_("evaluationTimeout", 24 * 3600 * 1000).E().hasLabel('LikesCoffee').has('country','Colombia').count().next()
        //g.with_("evaluationTimeout", 24 * 3600 * 1000).E().hasLabel('LikesCoffee').has('country',P.within('Colombia','Peru','Ecuador')).count().next()
        //g.with_("evaluationTimeout", 24 * 3600 * 1000).V().hasLabel('Person').where(__.out('HasDog').count().is_(P.gt(0))).out('LikesCoffee').has('country',P.within('Colombia','Peru','Ecuador')).next()

    }

    public List<Duration> executeCoffeeOrigin(GraphTraversalSource g) {
        final List<Duration> durations = new ArrayList<>();

        // 2_000_000 people
        for (int i = 0; i < 10; i++) {
            System.out.println("start");
            Instant start = Instant.now();
            //g.with("evaluationTimeout", 24 * 3600 * 1000).V().hasLabel("Person").count().next();
            // 2.2.0 -> 15 mins to do haslabel person count
            // 2.2.0 -> 20 mins to do haslabeloutlikescoffee count
            // 2.x-dev -> 9 minutes to do haslabel person count
            // 2.x-dev -> 18 minutes to do haslabeloutlikescoffee count

            // group on root fix.
            // label index enabled
            // page queue size 643
            // page size 10000
            // g.withComputer().with("evaluationTimeout", 24 * 3600 * 1000).V().hasLabel("Person").out("LikesCoffee").groupCount().by("country").toList();
            //

            System.out.println(g.withComputer().with("evaluationTimeout", 24 * 3600 * 1000).V().hasLabel("Person").limit(1000).out("LikesCoffee").toList());
            Instant end = Instant.now();
            Duration duration = Duration.between(start, end);
            durations.add(duration);
            System.out.println("Time: " + duration.getSeconds() + "s");
        }
        return durations;
    }
}
