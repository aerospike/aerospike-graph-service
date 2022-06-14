package com.aerospike.firefly.performance;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.process.TestAerospikeGraphIntegration;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.Assert;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.Util.loadKryoDataFromResources;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.out;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestPerformance {
    Logger logger = LoggerFactory.getLogger(TestAerospikeGraphIntegration.class);

    private static final Configuration config;

    static {
        config = ConfigurationHelper.loadFromResources(INTEGRATION_TEST_PROPERTIES);
    }

    private AerospikeConnection db;
    private FireflyGraph graph;
    private GraphTraversalSource g;

    @BeforeEach
    void openGraph() {
        this.db = AerospikeConnection.connect(config);
        graph = FireflyGraph.open(config);
        db.dropDatabase();
        g = graph.traversal();

    }

    @AfterEach
    void closeGraphClearData() throws Exception {
        db.dropDatabase();
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
        GraphHelper.cloneElements(TinkerFactory.createGratefulDead(),graph);
        long loadtime = stopTimer(LOAD_TIMER);
        System.out.println(String.format("load time for tinkerpop-grateful.kryo: %d ms", loadtime));
    }


    @Test
    void createAndIterateTree() {
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

}
