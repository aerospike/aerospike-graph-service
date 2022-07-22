package com.aerospike.firefly.performance;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.process.TestAerospikeGraphIntegration;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.aerospike.firefly.util.PerfUtil;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.LoadGraphWith;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.apache.tinkerpop.gremlin.process.traversal.Scope.local;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.outE;
import static org.junit.Assert.assertEquals;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestPerformance {
    Logger logger = LoggerFactory.getLogger(TestAerospikeGraphIntegration.class);

    private static final Configuration config;

    static {
        config = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
    }

    private static AerospikeConnection db;
    private static FireflyGraph graph;
    private static GraphTraversalSource g;

    @BeforeClass
    public static void openGraph() {
        db = AerospikeConnection.connect(config);
        graph = FireflyGraph.open(config);
        g = graph.traversal();
        g.V().drop().iterate();
    }

    @Before
    public void clearGraph() {
        try (final FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES))) {
            if (graph.traversal().V().count().next() > 0 || graph.traversal().E().count().next() > 0)
                LoggerFactory.getLogger("clearGraph").warn("nonzero vertex or edge count at start of test");
            graph.traversal().V().drop().iterate();
        }
    }

    @AfterClass
    public static void closeGraph() {
        db.close();
        graph.close();
    }

    public void printTraversalForm(final Traversal traversal) {
        logger.info("   pre-strategy:" + traversal);
        if (!traversal.asAdmin().isLocked()) traversal.asAdmin().applyStrategies();
        logger.info("  post-strategy:" + traversal);
    }

    private final Map<String, Long> timers = new HashMap<>();

    private void startTimer(String name) {
        timers.put(name, ZonedDateTime.now().toInstant().toEpochMilli());
    }

    private long stopTimer(String name) {
        return ZonedDateTime.now().toInstant().toEpochMilli() - timers.get(name);
    }

    private static final String LOAD_TIMER = "loadTimer";
    private static final String QUERY_TIMER = "queryTimer";

    @Test
    public void loadGratefulDataset() {
        startTimer(LOAD_TIMER);
        GraphHelper.cloneElements(TinkerFactory.createGratefulDead(), graph);
        long loadtime = stopTimer(LOAD_TIMER);
        System.out.println(String.format("load time for tinkerpop-grateful.kryo: %d ms", loadtime));
    }


    @Test
    public void createAndIterateTree() {
        final String ADD_ELEMENTS = "addElements";
        final String ITERATE_ELEMENTS = "iterateElements";
        startTimer(ADD_ELEMENTS);
        int branchSize = 11;
        long elements = 0;
        final Vertex start = graph.addVertex();
        for (int i = 0; i < branchSize; i++) {
            final Vertex a = graph.addVertex();
            start.addEdge("test1", a);
            elements++;
            for (int j = 0; j < branchSize; j++) {
                final Vertex b = graph.addVertex();
                a.addEdge("test2", b);
                elements++;
                for (int k = 0; k < branchSize; k++) {
                    final Vertex c = graph.addVertex();
                    b.addEdge("test3", c);
                    elements++;
                }
            }
        }
        long addElementsTime = stopTimer(ADD_ELEMENTS);
        assertEquals(0L, IteratorUtils.count(start.edges(Direction.IN, new String[0])));
        assertEquals((long) branchSize, IteratorUtils.count(start.edges(Direction.OUT, new String[0])));
        Iterator var9 = IteratorUtils.list(start.edges(Direction.OUT, new String[0])).iterator();
        AtomicLong iterCtr = new AtomicLong(0);
        startTimer(ITERATE_ELEMENTS);
        while (var9.hasNext()) {
            iterCtr.incrementAndGet();
            Edge a = (Edge) var9.next();
            Assert.assertEquals("test1", a.label());

            Assert.assertEquals((long) branchSize, IteratorUtils.count(a.inVertex().vertices(Direction.OUT, new String[0])));
            Assert.assertEquals(1L, IteratorUtils.count(a.inVertex().vertices(Direction.IN, new String[0])));
            Iterator var12 = IteratorUtils.list(a.inVertex().edges(Direction.OUT, new String[0])).iterator();

            while (var12.hasNext()) {
                iterCtr.incrementAndGet();
                Edge b = (Edge) var12.next();
                Assert.assertEquals("test2", b.label());
                Assert.assertEquals((long) branchSize, IteratorUtils.count(b.inVertex().vertices(Direction.OUT, new String[0])));
                Assert.assertEquals(1L, IteratorUtils.count(b.inVertex().vertices(Direction.IN, new String[0])));
                Iterator var14 = IteratorUtils.list(b.inVertex().edges(Direction.OUT, new String[0])).iterator();

                while (var14.hasNext()) {
                    iterCtr.incrementAndGet();
                    Edge c = (Edge) var14.next();
                    Assert.assertEquals("test3", c.label());
                    Assert.assertEquals(0L, IteratorUtils.count(c.inVertex().vertices(Direction.OUT, new String[0])));
                    Assert.assertEquals(1L, IteratorUtils.count(c.inVertex().vertices(Direction.IN, new String[0])));
                }
            }
        }
        long iterateTime = stopTimer(ITERATE_ELEMENTS);
        System.out.println(String.format("add %d elements time: %d ms", elements, addElementsTime));
        System.out.println(String.format("iterate time: %d ms", iterateTime));
        System.out.println(String.format("iterate count: %d", iterCtr.get()));

    }

    @Test
    public void test2hopRepeat1() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        PerfUtil.Results results = PerfUtil.runTestBatch(1000, () -> {
            List<Vertex> data = g.V().local(outE().limit(1)).inV().limit(3).toList();
            assert data.size() == 3;
        });
        System.out.println(results);
    }

    @Test
    public void test2hopRepeat2() {
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        PerfUtil.Results results = PerfUtil.runTestBatch(1000, () -> {
            List<Object> thing = g.V().as("a").out().as("b").out().as("c").<Map<String, String>>select("a", "b", "c").by("name").range(local, 1, 2).toList();
        });
        System.out.println(results);
    }

    private static final long ORG_CHART_VERTEX_COUNT = 12;
    private static final long ORG_CHART_EDGE_COUNT = 11;
    private static final String ORGCHART_VERTEX_LABEL_EMPLOYEE = "employee";
    private static final long ORGCHART_EMPLOYEE_COUNT = 7;
    private static final String ORGCHART_VERTEX_LABEL_MANAGER = "manager";

    private static final String ORGCHART_EDGE_LABEL_REPORTS = "reportsTo";
    private static final String ORGCHART_EDGE_LABEL_CONTRACTS = "contractsTo";

    private static final String ORGCHART_NAME = "name";
    private static final String ORGCHART_TITLE = "title";

    private void createOrgChartData() {
        Vertex p1 = g.addV(ORGCHART_VERTEX_LABEL_EMPLOYEE).property(ORGCHART_NAME, "alice").property(ORGCHART_TITLE, "worker").next();
        Vertex p2 = g.addV(ORGCHART_VERTEX_LABEL_EMPLOYEE).property(ORGCHART_NAME, "bob").property(ORGCHART_TITLE, "worker").next();
        Vertex p3 = g.addV(ORGCHART_VERTEX_LABEL_MANAGER).property(ORGCHART_NAME, "carol").property(ORGCHART_TITLE, "manager").next();
        Vertex p4 = g.addV(ORGCHART_VERTEX_LABEL_MANAGER).property(ORGCHART_NAME, "dean").property(ORGCHART_TITLE, "manager").next();
        Vertex p5 = g.addV(ORGCHART_VERTEX_LABEL_EMPLOYEE).property(ORGCHART_NAME, "evelyn").property(ORGCHART_TITLE, "worker").next();
        Vertex p6 = g.addV(ORGCHART_VERTEX_LABEL_EMPLOYEE).property(ORGCHART_NAME, "frank").property(ORGCHART_TITLE, "worker").next();
        Vertex p7 = g.addV(ORGCHART_VERTEX_LABEL_EMPLOYEE).property(ORGCHART_NAME, "gary").property(ORGCHART_TITLE, "worker").next();
        Vertex p8 = g.addV(ORGCHART_VERTEX_LABEL_EMPLOYEE).property(ORGCHART_NAME, "harry").property(ORGCHART_TITLE, "worker").next();
        Vertex p9 = g.addV(ORGCHART_VERTEX_LABEL_EMPLOYEE).property(ORGCHART_NAME, "ivan").property(ORGCHART_TITLE, "worker").next();
        Vertex p10 = g.addV(ORGCHART_VERTEX_LABEL_MANAGER).property(ORGCHART_NAME, "jack").property(ORGCHART_TITLE, "ceo").next();
        Vertex p11 = g.addV(ORGCHART_VERTEX_LABEL_MANAGER).property(ORGCHART_NAME, "kris").property(ORGCHART_TITLE, "vp").next();
        Vertex p12 = g.addV(ORGCHART_VERTEX_LABEL_MANAGER).property(ORGCHART_NAME, "lance").property(ORGCHART_TITLE, "vp").next();

        g.addE(ORGCHART_EDGE_LABEL_REPORTS).from(p1).to(p3);
        g.addE(ORGCHART_EDGE_LABEL_REPORTS).from(p2).to(p3);
        g.addE(ORGCHART_EDGE_LABEL_REPORTS).from(p3).to(p12);
        g.addE(ORGCHART_EDGE_LABEL_REPORTS).from(p4).to(p11);
        g.addE(ORGCHART_EDGE_LABEL_REPORTS).from(p5).to(p4);
        g.addE(ORGCHART_EDGE_LABEL_REPORTS).from(p6).to(p4);
        g.addE(ORGCHART_EDGE_LABEL_REPORTS).from(p7).to(p4);
        g.addE(ORGCHART_EDGE_LABEL_REPORTS).from(p8).to(p3);
        g.addE(ORGCHART_EDGE_LABEL_REPORTS).from(p9).to(p3);
        g.addE(ORGCHART_EDGE_LABEL_CONTRACTS).from(p11).to(p10);
        g.addE(ORGCHART_EDGE_LABEL_REPORTS).from(p12).to(p10);
    }

    @Test
    public void orgchartReadWriteAccounting1() {
        final long readStart = db.getReadMetric();
        final long writeStart = db.getWriteMetric();

        createOrgChartData();
        assertEquals(36, db.getWriteMetric() - writeStart);
        List<Object> result1 = g.V()
                .has(ORGCHART_VERTEX_LABEL_EMPLOYEE, ORGCHART_NAME, "bob")
                .in(ORGCHART_EDGE_LABEL_REPORTS)
                .in(ORGCHART_EDGE_LABEL_REPORTS).values(ORGCHART_NAME).toList();
        final long result1ReadMetric = db.getReadMetric();
        // when using label, reads are much higher
        assertEquals(40, result1ReadMetric - readStart);

        List<Object> result2 = g.V()
                .has(ORGCHART_NAME, "bob")
                .in(ORGCHART_EDGE_LABEL_REPORTS)
                .in(ORGCHART_EDGE_LABEL_REPORTS).values(ORGCHART_NAME).toList();
        final long result2ReadMetric = db.getReadMetric();
        assertEquals(6, result2ReadMetric - result1ReadMetric);
        assertEquals(result1, result2);

        List<Vertex> result3 = g.V().has(ORGCHART_VERTEX_LABEL_EMPLOYEE, ORGCHART_NAME, "ivan").toList();
        final long result3ReadMetric = db.getReadMetric();
        assertEquals(14, result3ReadMetric - result2ReadMetric);
        List<Vertex> result4 = g.V().has(ORGCHART_NAME, "ivan").toList();
        final long result4ReadMetric = db.getReadMetric();
        assertEquals(4, result4ReadMetric - result3ReadMetric);
        assertEquals(result3, result4);
    }

    @Test
    public void testLabelQuery() {
        createOrgChartData();
        final long startReadMetric = db.getReadMetric();
        List<Vertex> x = g.V().hasLabel(ORGCHART_VERTEX_LABEL_EMPLOYEE).toList();
        final long afterGetByLabelMetric = db.getReadMetric();
        //results by index, no reads
        assertEquals(0, afterGetByLabelMetric - startReadMetric);
    }

    @Test
    public void grateful_V_out_out_profile() {
        Traversal<Vertex, TraversalMetrics> traversal = g.V().out().out().profile();
        this.printTraversalForm(traversal);
        TraversalMetrics traversalMetrics = (TraversalMetrics) traversal.next();
        Collection<? extends Metrics> m = traversalMetrics.getMetrics();
        assertEquals(4, m.size());
    }
}
