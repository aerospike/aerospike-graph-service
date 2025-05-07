package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class FireflyAdjacentVertexIdStepTest {
    static private Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    static private FireflyGraph GRAPH;
    private List<Object> inIds;
    private List<Object> outIds;

    @BeforeClass
    static public void beforeAll() {
        GRAPH = FireflyGraph.open(CONFIG);
        GRAPH.getBaseGraph().dropDatabase(GRAPH, true);
        GRAPH.close();
        CONFIG.setProperty(ConfigurationHelper.Keys.ENABLE_CACHED_ADJACENT_ID_STRATEGY, "true");
        CONFIG.setProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, "2");
        GRAPH = FireflyGraph.open(CONFIG);
    }

    @AfterClass
    static public void afterAll() {
        GRAPH.getBaseGraph().dropDatabase(GRAPH, true);
        GRAPH.close();
    }

    @Before
    public void beforeEach() {
        GRAPH.getBaseGraph().dropDatabase(GRAPH, false);
        inIds = new ArrayList<>();
        outIds = new ArrayList<>();
        final var g = GRAPH.traversal();
        final Vertex inV = g.addV("inSource").next();
        final Vertex outV = g.addV("outSource").next();
        inIds.add(g.addV().next().id());
        inIds.add(g.addV().next().id());
        inIds.add(g.addV().next().id());
        inIds.add(g.addV().next().id());
        outIds.add(g.addV().next().id());
        outIds.add(g.addV().next().id());
        outIds.add(g.addV().next().id());
        outIds.add(g.addV().next().id());
        for (int i = 0; i < 4; i++) {
            final String edgeLabel = "e" + i;
            g.addE(edgeLabel).from(__.V(inIds.get(i))).to(inV).iterate();
            g.addE(edgeLabel).from(outV).to(__.V(outIds.get(i))).iterate();
        }
    }

    @After
    public void afterEacb() {
        GRAPH.getBaseGraph().dropDatabase(GRAPH, false);
    }

    @Test
    public void testNoLabel() {
        final var g = GRAPH.traversal();
        var t = g.V().hasLabel("inSource").in().id();
        Assert.assertEquals(4, inIds.size());
        int count = 0;
        while (t.hasNext()) {
            count++;
            inIds.remove(t.next());
        }
        Assert.assertEquals(0, inIds.size());
        Assert.assertEquals(4, count);

        t = g.V().hasLabel("outSource").out().id();
        Assert.assertEquals(4, outIds.size());
        count = 0;
        while (t.hasNext()) {
            count++;
            outIds.remove(t.next());
        }
        Assert.assertEquals(0, outIds.size());
        Assert.assertEquals(4, count);
    }

    @Test
    public void testWithLabel() {
        final var g = GRAPH.traversal();
        var t = g.V().hasLabel("inSource").in("e0").id();
        List<Object> testIds = new ArrayList<>(inIds);
        Assert.assertEquals(4, testIds.size());
        int count = 0;
        while (t.hasNext()) {
            count++;
            testIds.remove(t.next());
        }
        Assert.assertEquals(3, testIds.size());
        Assert.assertEquals(1, count);

        t = g.V().hasLabel("inSource").in("e0", "e1").id();
        testIds = new ArrayList<>(inIds);
        Assert.assertEquals(4, testIds.size());
        count = 0;
        while (t.hasNext()) {
            count++;
            testIds.remove(t.next());
        }
        Assert.assertEquals(2, testIds.size());
        Assert.assertEquals(2, count);

        t = g.V().hasLabel("inSource").in("e1", "e2").id();
        testIds = new ArrayList<>(inIds);
        Assert.assertEquals(4, testIds.size());
        count = 0;
        while (t.hasNext()) {
            count++;
            testIds.remove(t.next());
        }
        Assert.assertEquals(2, testIds.size());
        Assert.assertEquals(2, count);

        t = g.V().hasLabel("inSource").in("e2", "e3").id();
        testIds = new ArrayList<>(inIds);
        Assert.assertEquals(4, testIds.size());
        count = 0;
        while (t.hasNext()) {
            count++;
            testIds.remove(t.next());
        }
        Assert.assertEquals(2, testIds.size());
        Assert.assertEquals(2, count);

        t = g.V().hasLabel("outSource").out("e0").id();
        testIds = new ArrayList<>(outIds);
        Assert.assertEquals(4, testIds.size());
        count = 0;
        while (t.hasNext()) {
            count++;
            testIds.remove(t.next());
        }
        Assert.assertEquals(3, testIds.size());
        Assert.assertEquals(1, count);

        t = g.V().hasLabel("outSource").out("e0", "e1").id();
        testIds = new ArrayList<>(outIds);
        Assert.assertEquals(4, testIds.size());
        count = 0;
        while (t.hasNext()) {
            count++;
            testIds.remove(t.next());
        }
        Assert.assertEquals(2, testIds.size());
        Assert.assertEquals(2, count);

        t = g.V().hasLabel("outSource").out("e1", "e2").id();
        testIds = new ArrayList<>(outIds);
        Assert.assertEquals(4, testIds.size());
        count = 0;
        while (t.hasNext()) {
            count++;
            testIds.remove(t.next());
        }
        Assert.assertEquals(2, testIds.size());
        Assert.assertEquals(2, count);

        t = g.V().hasLabel("outSource").out("e2", "e3").id();
        testIds = new ArrayList<>(outIds);
        Assert.assertEquals(4, testIds.size());
        count = 0;
        while (t.hasNext()) {
            count++;
            testIds.remove(t.next());
        }
        Assert.assertEquals(2, testIds.size());
        Assert.assertEquals(2, count);
    }
}
