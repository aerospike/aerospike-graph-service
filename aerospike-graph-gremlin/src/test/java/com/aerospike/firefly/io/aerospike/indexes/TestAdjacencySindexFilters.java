package com.aerospike.firefly.io.aerospike.indexes;

import com.aerospike.client.Record;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIteratorFromVertex;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.google.common.collect.Iterators;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.GLOBAL_EDGE_CACHE_ENABLED;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.PHAT_EDGE_SIZE;

public class TestAdjacencySindexFilters {
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    static {
        CONFIG.setProperty(GLOBAL_EDGE_CACHE_ENABLED, "false");
        CONFIG.setProperty(PHAT_EDGE_SIZE, "1");
    }

    private FireflyGraph graph;
    private Vertex v1;
    private Vertex v2;

    @BeforeClass
    static public void beforeAll() {
        // Create a default graph to clean up non-mutable configurations just in case.
        try (final FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            graph.getBaseGraph().dropDatabase(graph, true);
        }
    }

    @Before
    public void beforeEach() {
        graph = FireflyGraph.open(CONFIG);
        graph.getBaseGraph().dropDatabase(graph, true);
        v1 = graph.traversal().addV("v1").next();
        v2 = graph.traversal().addV("v2").next();
    }

    @After
    public void afterEach() {
        if (graph != null) {
            graph.getBaseGraph().dropDatabase(graph, true);
            graph.close();
        }
    }

    @Test
    public void testLabelPushdown() {
        GraphTraversalSource g = graph.traversal();
        g.addE("e1").from(v1).to(v2).iterate();
        g.addE("e2").from(v1).to(v2).iterate();
        g.addE("e3").from(v1).to(v2).iterate();

        FireflyVertex v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e1", "e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, Collections.emptyList())));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e3"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, Collections.emptyList())));
        Assert.assertEquals(3, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, Collections.emptyList())));

        FireflyVertex v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e1", "e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, Collections.emptyList())));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e3"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, Collections.emptyList())));
        Assert.assertEquals(3, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, Collections.emptyList())));
    }

    @Test
    public void testPropertyEqPushdown() {
        GraphTraversalSource g = graph.traversal();
        g.addE("e1").property("foo", "Simon").from(v1).to(v2).iterate();
        g.addE("e2").property("foo", "Lyndon").from(v1).to(v2).iterate();
        g.addE("e3").property("foo", 100).from(v1).to(v2).iterate();
        g.addE("e4").property("foo", 100).from(v1).to(v2).iterate();
        g.addE("e5").property("foo", 200).from(v1).to(v2).iterate();
        g.addE("e6").property("bar", "Lyndon").from(v1).to(v2).iterate();
        g.addE("e7").property("foo", 100).property("bar", "Lyndon").from(v1).to(v2).iterate();
        String e7id = (String) g.E().hasLabel("e7").next().id();
        String e8id = (String) g.addE("e8").from(v1).to(v2).next().id();
        g.E().hasLabel("e8").drop().iterate();

        HasContainer hasSimon = new HasContainer("foo", P.eq("Simon"));
        HasContainer hasLyndon = new HasContainer("bar", P.eq("Lyndon"));
        HasContainer has100 = new HasContainer("foo", P.eq(100));
        HasContainer hasId = new HasContainer(T.id.getAccessor(), P.eq(e7id));
        HasContainer hasIdFalse = new HasContainer(T.id.getAccessor(), P.eq(e8id));

        FireflyVertex v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(3, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasId))));
        Assert.assertEquals(0, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasIdFalse))));

        FireflyVertex v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(3, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasId))));
        Assert.assertEquals(0, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasIdFalse))));
    }

    @Test
    public void testPropertyAndLabelPushdown() {
        GraphTraversalSource g = graph.traversal();
        g.addE("e1").property("foo", "Simon").from(v1).to(v2).iterate();
        g.addE("e2").property("foo", "Lyndon").from(v1).to(v2).iterate();
        g.addE("e3").property("foo", 100).from(v1).to(v2).iterate();
        g.addE("e4").property("foo", 100).from(v1).to(v2).iterate();
        g.addE("e5").property("foo", 200).from(v1).to(v2).iterate();
        g.addE("e6").property("bar", "Lyndon").from(v1).to(v2).iterate();
        g.addE("e7").property("foo", 100).property("bar", "Lyndon").from(v1).to(v2).iterate();

        HasContainer hasSimon = new HasContainer("foo", P.eq("Simon"));
        HasContainer hasLyndon = new HasContainer("bar", P.eq("Lyndon"));
        HasContainer has100 = new HasContainer("foo", P.eq(100));

        FireflyVertex v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e1", "e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(0, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(3, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e3", "e4", "e7", "e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e3", "e7"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e7", "e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasLyndon))));
        Assert.assertEquals(0, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e3", "e4", "e6"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e7"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e7", "e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));

        FireflyVertex v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e1", "e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(0, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(3, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e3", "e4", "e7", "e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e3", "e7"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e7", "e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasLyndon))));
        Assert.assertEquals(0, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e3", "e4", "e6"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e7"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e7", "e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));
    }

    @Test
    public void testPropertyAndLabelPushdownPropertyAddedAfterCreation() {
        GraphTraversalSource g = graph.traversal();
        g.addE("e1").from(v1).to(v2).iterate();
        g.E().hasLabel("e1").property("foo", "Simon").iterate();
        g.addE("e2").from(v1).to(v2).iterate();
        g.E().hasLabel("e2").property("foo", "Lyndon").iterate();
        g.addE("e3").from(v1).to(v2).iterate();
        g.E().hasLabel("e3").property("foo", 100).iterate();
        g.addE("e4").from(v1).to(v2).iterate();
        g.E().hasLabel("e4").property("foo", 100).iterate();
        g.addE("e5").from(v1).to(v2).iterate();
        g.E().hasLabel("e5").property("foo", 200).iterate();
        g.addE("e6").from(v1).to(v2).iterate();
        g.E().hasLabel("e6").property("bar", "Lyndon").iterate();
        g.addE("e7").from(v1).to(v2).iterate();
        g.E().hasLabel("e7").property("foo", 100).property("bar", "Lyndon").iterate();

        HasContainer hasSimon = new HasContainer("foo", P.eq("Simon"));
        HasContainer hasLyndon = new HasContainer("bar", P.eq("Lyndon"));
        HasContainer has100 = new HasContainer("foo", P.eq(100));

        FireflyVertex v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e1", "e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(0, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(3, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e3", "e4", "e7", "e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e3", "e7"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e7", "e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasLyndon))));
        Assert.assertEquals(0, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e3", "e4", "e6"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e7"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e7", "e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));

        FireflyVertex v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e1", "e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(0, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(3, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e3", "e4", "e7", "e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e3", "e7"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e7", "e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasLyndon))));
        Assert.assertEquals(0, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e3", "e4", "e6"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e7"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e7", "e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));
    }

    @Test
    public void testPropertyComparisonPushdown() {
        GraphTraversalSource g = graph.traversal();
        g.addE("e1").property("foo", 100).from(v1).to(v2).iterate();
        g.addE("e1").property("foo", "100").from(v1).to(v2).iterate();
        g.addE("e2").property("foo", 200).from(v1).to(v2).iterate();
        g.addE("e2").property("foo", "200").from(v1).to(v2).iterate();
        g.addE("e3").property("foo", 300).from(v1).to(v2).iterate();
        g.addE("e3").property("bar", "300").from(v1).to(v2).iterate();
        g.addE("e4").property("foo", 400).property("bar", 200).from(v1).to(v2).iterate();
        g.addE("e4").property("foo", "400").property("bar", 200).from(v1).to(v2).iterate();

        HasContainer gt100 = new HasContainer("foo", P.gt(100));
        HasContainer gte100 = new HasContainer("foo", P.gte(100));
        HasContainer lt400 = new HasContainer("foo", P.lt(400));
        HasContainer lte400 = new HasContainer("foo", P.lte(400));
        HasContainer barGt150 = new HasContainer("bar", P.gt(150));
        HasContainer barGt250 = new HasContainer("bar", P.gt(250));
        HasContainer barLt150 = new HasContainer("bar", P.lt(150));
        HasContainer barLt250 = new HasContainer("bar", P.lt(250));

        FireflyVertex v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        Assert.assertEquals(3, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(gt100))));
        Assert.assertEquals(4, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(gte100))));
        Assert.assertEquals(3, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(gte100, gt100))));
        Assert.assertEquals(3, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(lt400))));
        Assert.assertEquals(4, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(lte400))));
        Assert.assertEquals(3, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(lte400, lt400))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(gt100, lt400))));
        Assert.assertEquals(3, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(gte100, lt400))));
        Assert.assertEquals(3, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(gt100, lte400))));
        Assert.assertEquals(4, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(gte100, lte400))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(gt100, lte400, barGt150, barLt250))));
        Assert.assertEquals(0, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(gt100, lte400, barGt250, barLt150))));

        FireflyVertex v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        Assert.assertEquals(3, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(gt100))));
        Assert.assertEquals(4, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(gte100))));
        Assert.assertEquals(3, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(gte100, gt100))));
        Assert.assertEquals(3, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(lt400))));
        Assert.assertEquals(4, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(lte400))));
        Assert.assertEquals(3, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(lte400, lt400))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(gt100, lt400))));
        Assert.assertEquals(3, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(gte100, lt400))));
        Assert.assertEquals(3, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(gt100, lte400))));
        Assert.assertEquals(4, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(gte100, lte400))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(gt100, lte400, barGt150, barLt250))));
        Assert.assertEquals(0, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(gt100, lte400, barGt250, barLt150))));
    }

    @Test
    public void testPushdownDisabledCompatibility() {
        if (Integer.parseInt(FireflyGraph.dataModelVersion().toString().split("\\.")[0]) >= 3) {
            throw new UnsupportedOperationException("Check if this test still makes sense on AGS > 2.x.x");
        }
        this.graph.getBaseGraph().setGraphMetadata("packed", "2.0.0");
        // Need to close graph since underlying AerospikeConnection is shared
        this.graph.close();

        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            // Ensure that seeing an existing 2.0.0 data model disables pushdown
            Assert.assertFalse(graph.getBaseGraph().isSupernodePushdownEnabled);

            // Write data using 2.0.0 data model compatibility mode
            var g = graph.traversal();
            g.addE("e1").from(__.V(v1.id())).to(__.V(v2.id())).iterate();
            g.E().hasLabel("e1").property("foo", 100).iterate();
            g.addE("e1").property("foo", "100").from(__.V(v1.id())).to(__.V(v2.id())).iterate();
            g.addE("e2").from(__.V(v1.id())).to(__.V(v2.id())).iterate();
            g.E().hasLabel("e2").property("foo", 200).iterate();
            g.addE("e2").property("foo", "200").from(__.V(v1.id())).to(__.V(v2.id())).iterate();

            // Check reading using compatibility mode
            var traversal = g.V().outE().has("foo", "100");
            Assert.assertEquals(1, Iterators.size(traversal));
            traversal = g.V().outE().has("foo", P.gt(99));
            Assert.assertEquals(2, Iterators.size(traversal));

            // Reset the version metadata to the actual version
            graph.getBaseGraph().setGraphMetadata(graph.getDataModel(), FireflyGraph.dataModelVersion().toString());
        }

        try (final FireflyGraph graph = FireflyGraph.open(CONFIG)) {
            // Ensure that pushdown was re-enabled
            Assert.assertTrue(graph.getBaseGraph().isSupernodePushdownEnabled);

            // See that the previous writes done in compatibility mode did not write to pushdown bins
            var g = graph.traversal();
            var traversal = g.V().outE().has("foo", "100");
            Assert.assertFalse(traversal.hasNext());
            traversal = g.V().outE().has("foo", P.gt(99));
            Assert.assertFalse(traversal.hasNext());
        }

        this.graph = FireflyGraph.open(CONFIG);
    }

    @Test
    public void testPropertyRemovePushdown() {
        GraphTraversalSource g = graph.traversal();
        g.addE("e1").property("foo", "Simon").from(v1).to(v2).iterate();
        g.addE("e2").property("foo", "Lyndon").from(v1).to(v2).iterate();
        g.addE("e3").property("foo", 100).from(v1).to(v2).iterate();
        g.addE("e4").property("foo", 100).from(v1).to(v2).iterate();
        g.E().hasLabel("e4").properties("foo").drop().iterate();
        g.addE("e5").property("foo", 200).from(v1).to(v2).iterate();
        g.addE("e6").property("bar", "Lyndon").from(v1).to(v2).iterate();
        g.addE("e7").property("foo", 100).property("bar", "Lyndon").from(v1).to(v2).iterate();

        HasContainer hasSimon = new HasContainer("foo", P.eq("Simon"));
        HasContainer hasLyndon = new HasContainer("bar", P.eq("Lyndon"));
        HasContainer has100 = new HasContainer("foo", P.eq(100));

        FireflyVertex v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));

        FireflyVertex v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));
    }

    @Test
    public void testPropertyUpdatePushdown() {
        GraphTraversalSource g = graph.traversal();
        g.addE("e1").property("foo", "Simon").from(v1).to(v2).iterate();
        g.addE("e2").property("foo", "Lyndon").from(v1).to(v2).iterate();
        g.addE("e3").property("foo", 100).from(v1).to(v2).iterate();
        g.addE("e4").property("foo", 100).from(v1).to(v2).iterate();
        g.E().hasLabel("e4").property("foo", 200).iterate();
        g.addE("e5").property("foo", 200).from(v1).to(v2).iterate();
        g.addE("e6").property("bar", "Lyndon").from(v1).to(v2).iterate();
        g.addE("e7").property("foo", 100).property("bar", "Lyndon").from(v1).to(v2).iterate();

        HasContainer hasSimon = new HasContainer("foo", P.eq("Simon"));
        HasContainer hasLyndon = new HasContainer("bar", P.eq("Lyndon"));
        HasContainer has100 = new HasContainer("foo", P.eq(100));
        HasContainer has200 = new HasContainer("foo", P.eq(200));

        FireflyVertex v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has200))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));

        FireflyVertex v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has200))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));
    }

    @Test
    public void testPropertyUpdatePushdownInvalidPushdown() {
        GraphTraversalSource g = graph.traversal();
        g.addE("e1").property("foo", "Simon").from(v1).to(v2).iterate();
        g.addE("e2").property("foo", "Lyndon").from(v1).to(v2).iterate();
        g.addE("e3").property("foo", 100).from(v1).to(v2).iterate();
        g.addE("e4").property("foo", 100).from(v1).to(v2).iterate();
        g.E().hasLabel("e4").property("foo", false).iterate();
        g.addE("e5").property("foo", 200).from(v1).to(v2).iterate();
        g.addE("e6").property("bar", "Lyndon").from(v1).to(v2).iterate();
        g.addE("e7").property("foo", 100).property("bar", "Lyndon").from(v1).to(v2).iterate();

        HasContainer hasSimon = new HasContainer("foo", P.eq("Simon"));
        HasContainer hasLyndon = new HasContainer("bar", P.eq("Lyndon"));
        HasContainer has100 = new HasContainer("foo", P.eq(100));
        HasContainer has200 = new HasContainer("foo", P.eq(200));

        FireflyVertex v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has200))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));

        FireflyVertex v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has200))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));
    }

    @Test
    public void testPropertyNullPushdown() {
        GraphTraversalSource g = graph.traversal();
        g.addE("e1").property("foo", "Simon").from(v1).to(v2).iterate();
        g.addE("e2").property("foo", "Lyndon").from(v1).to(v2).iterate();
        g.addE("e3").property("foo", 100).from(v1).to(v2).iterate();
        g.addE("e4").property("foo", 100).from(v1).to(v2).iterate();
        g.E().hasLabel("e4").property("foo", null).iterate();
        g.addE("e5").property("foo", 200).from(v1).to(v2).iterate();
        g.addE("e6").property("bar", "Lyndon").from(v1).to(v2).iterate();
        g.addE("e7").property("foo", 100).property("bar", "Lyndon").from(v1).to(v2).iterate();

        HasContainer hasSimon = new HasContainer("foo", P.eq("Simon"));
        HasContainer hasLyndon = new HasContainer("bar", P.eq("Lyndon"));
        HasContainer has100 = new HasContainer("foo", P.eq(100));

        FireflyVertex v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));

        FireflyVertex v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasSimon))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasLyndon))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasLyndon))));
    }

    @Test
    public void testRemoveEdgePushdown() {
        GraphTraversalSource g = graph.traversal();
        g.addE("e1").property("foo", "bar").from(v1).to(v2).iterate();
        g.addE("e2").property("foo", "bar").from(v1).to(v2).iterate();

        HasContainer hasBar = new HasContainer("foo", P.eq("bar"));
        FireflyVertex v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBar))));

        FireflyVertex v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBar))));

        g.E().hasLabel("e1").drop().iterate();
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBar))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBar))));
    }

    @Test
    public void testWithinPhatEdgePushdown() {
        Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        conf.setProperty(GLOBAL_EDGE_CACHE_ENABLED, "false");
        try (final FireflyGraph phatFirefly = FireflyGraph.open(conf)) {
            GraphTraversalSource g = phatFirefly.traversal();
            FireflyVertex v1 = (FireflyVertex) g.V(this.v1.id()).next();
            FireflyVertex v2 = (FireflyVertex) g.V(this.v2.id()).next();
            FireflyVertex v3 = (FireflyVertex) g.addV("v3").next();
            FireflyVertex v4 = (FireflyVertex) g.addV("v4").next();
            g.addE("e1").property("fromto", "v1v2").from(v1).to(v2).iterate();
            g.addE("e2").property("fromto", "v1v2").from(v1).to(v2).iterate();
            g.addE("e3").property("fromto", "v3v4").from(v3).to(v4).iterate();

            HasContainer hasv1v2= new HasContainer("fromto", P.eq("v1v2"));
            HasContainer hasv3v4 = new HasContainer("fromto", P.eq("v3v4"));
            Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv1v2))));
            Assert.assertEquals(0, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv3v4))));
            Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv1v2))));
            Assert.assertEquals(0, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv3v4))));
            Assert.assertEquals(0, Iterators.size(v3.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv1v2))));
            Assert.assertEquals(1, Iterators.size(v3.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv3v4))));
            Assert.assertEquals(0, Iterators.size(v4.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv1v2))));
            Assert.assertEquals(1, Iterators.size(v4.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv3v4))));
            Record record = v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, Collections.emptyList()).next().record;
            Map<String, Map<String, Map<Long, Object>>> supernodePMap = (Map<String, Map<String, Map<Long, Object>>>) record.getMap(phatFirefly.getBaseGraph().SUPERNODE_EDGE_PROPERTIES_BIN);
            Assert.assertTrue(supernodePMap.containsKey(v1.id.getKeyHashString()));
            Assert.assertTrue(supernodePMap.containsKey(v2.id.getKeyHashString()));
            Assert.assertTrue(supernodePMap.containsKey(v3.id.getKeyHashString()));
            Assert.assertTrue(supernodePMap.containsKey(v4.id.getKeyHashString()));
            Assert.assertEquals(3, supernodePMap.get(v1.id.getKeyHashString()).size());
            Assert.assertEquals(3, supernodePMap.get(v2.id.getKeyHashString()).size());
            Assert.assertEquals(3, supernodePMap.get(v3.id.getKeyHashString()).size());
            Assert.assertEquals(3, supernodePMap.get(v4.id.getKeyHashString()).size());
            Assert.assertEquals(2, supernodePMap.get(v1.id.getKeyHashString()).get("fromto").size());
            Assert.assertEquals(2, supernodePMap.get(v1.id.getKeyHashString()).get("~label").size());
            Assert.assertEquals(2, supernodePMap.get(v2.id.getKeyHashString()).get("fromto").size());
            Assert.assertEquals(2, supernodePMap.get(v2.id.getKeyHashString()).get("~label").size());
            Assert.assertEquals(1, supernodePMap.get(v3.id.getKeyHashString()).get("fromto").size());
            Assert.assertEquals(1, supernodePMap.get(v3.id.getKeyHashString()).get("~label").size());
            Assert.assertEquals(1, supernodePMap.get(v4.id.getKeyHashString()).get("fromto").size());
            Assert.assertEquals(1, supernodePMap.get(v4.id.getKeyHashString()).get("~label").size());

            // Removing an edge from the edge pack still works if there are other edges that match the pushdown
            g.E().hasLabel("e1").drop().iterate();
            Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv1v2))));
            Assert.assertEquals(0, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv3v4))));
            Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv1v2))));
            Assert.assertEquals(0, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv3v4))));
            Assert.assertEquals(0, Iterators.size(v3.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv1v2))));
            Assert.assertEquals(1, Iterators.size(v3.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv3v4))));
            Assert.assertEquals(0, Iterators.size(v4.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv1v2))));
            Assert.assertEquals(1, Iterators.size(v4.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv3v4))));
            record = v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, Collections.emptyList()).next().record;
            supernodePMap = (Map<String, Map<String, Map<Long, Object>>>) record.getMap(phatFirefly.getBaseGraph().SUPERNODE_EDGE_PROPERTIES_BIN);
            Assert.assertTrue(supernodePMap.containsKey(v1.id.getKeyHashString()));
            Assert.assertTrue(supernodePMap.containsKey(v2.id.getKeyHashString()));
            Assert.assertTrue(supernodePMap.containsKey(v3.id.getKeyHashString()));
            Assert.assertTrue(supernodePMap.containsKey(v4.id.getKeyHashString()));
            Assert.assertEquals(3, supernodePMap.get(v1.id.getKeyHashString()).size());
            Assert.assertEquals(3, supernodePMap.get(v2.id.getKeyHashString()).size());
            Assert.assertEquals(3, supernodePMap.get(v3.id.getKeyHashString()).size());
            Assert.assertEquals(3, supernodePMap.get(v4.id.getKeyHashString()).size());
            Assert.assertEquals(1, supernodePMap.get(v1.id.getKeyHashString()).get("fromto").size());
            Assert.assertEquals(1, supernodePMap.get(v1.id.getKeyHashString()).get("~label").size());
            Assert.assertEquals(1, supernodePMap.get(v2.id.getKeyHashString()).get("fromto").size());
            Assert.assertEquals(1, supernodePMap.get(v2.id.getKeyHashString()).get("~label").size());
            Assert.assertEquals(1, supernodePMap.get(v3.id.getKeyHashString()).get("fromto").size());
            Assert.assertEquals(1, supernodePMap.get(v3.id.getKeyHashString()).get("~label").size());
            Assert.assertEquals(1, supernodePMap.get(v4.id.getKeyHashString()).get("fromto").size());
            Assert.assertEquals(1, supernodePMap.get(v4.id.getKeyHashString()).get("~label").size());

            // Test removing an edge from the edge pack works properly if there are no edges that match the pushdown
            g.E().hasLabel("e3").drop().iterate();
            Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv1v2))));
            Assert.assertEquals(0, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv3v4))));
            Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv1v2))));
            Assert.assertEquals(0, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv3v4))));
            Assert.assertEquals(0, Iterators.size(v3.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv1v2))));
            Assert.assertEquals(0, Iterators.size(v3.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv3v4))));
            Assert.assertEquals(0, Iterators.size(v4.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv1v2))));
            Assert.assertEquals(0, Iterators.size(v4.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasv3v4))));
            record = v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, Collections.emptyList()).next().record;
            supernodePMap = (Map<String, Map<String, Map<Long, Object>>>) record.getMap(phatFirefly.getBaseGraph().SUPERNODE_EDGE_PROPERTIES_BIN);
            Assert.assertTrue(supernodePMap.containsKey(v1.id.getKeyHashString()));
            Assert.assertTrue(supernodePMap.containsKey(v2.id.getKeyHashString()));
            Assert.assertFalse(supernodePMap.containsKey(v3.id.getKeyHashString()));
            Assert.assertFalse(supernodePMap.containsKey(v4.id.getKeyHashString()));
            Assert.assertEquals(3, supernodePMap.get(v1.id.getKeyHashString()).size());
            Assert.assertEquals(3, supernodePMap.get(v2.id.getKeyHashString()).size());
            Assert.assertEquals(1, supernodePMap.get(v1.id.getKeyHashString()).get("fromto").size());
            Assert.assertEquals(1, supernodePMap.get(v1.id.getKeyHashString()).get("~label").size());
            Assert.assertEquals(1, supernodePMap.get(v2.id.getKeyHashString()).get("fromto").size());
            Assert.assertEquals(1, supernodePMap.get(v2.id.getKeyHashString()).get("~label").size());
        }
    }

    @Test
    public void testDropWithinPhatEdgeConcurrentModify() {
        // This test is for handling concurrency when an edge property is added by a different traversal while dropping
        Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        conf.setProperty(GLOBAL_EDGE_CACHE_ENABLED, "false");
        try (final FireflyGraph phatFirefly = FireflyGraph.open(conf)) {
            GraphTraversalSource g = phatFirefly.traversal();
            FireflyVertex v1 = (FireflyVertex) g.V(this.v1.id()).next();
            FireflyVertex v2 = (FireflyVertex) g.V(this.v2.id()).next();
            // Get handle on e1
            FireflyEdge e1 = (FireflyEdge) g.addE("e1").property("foo", "bar").from(v1).to(v2).next();
            g.addE("e2").property("foo", "bar").from(v1).to(v2).iterate();
            FireflyEdge concurrentE1 = (FireflyEdge) g.E().hasLabel("e1").property("culprit", "concurrentScoundrel").next();
            // See that e1 and e2 have different cached properties that are used to generate the removal operations
            Assert.assertEquals(e1.property("culprit"), Property.empty());
            Assert.assertNotEquals(concurrentE1.property("culprit"), Property.empty());

            Record record = v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, Collections.emptyList()).next().record;
            Map<String, Map<String, Map<Long, Object>>> supernodePMap = (Map<String, Map<String, Map<Long, Object>>>) record.getMap(phatFirefly.getBaseGraph().SUPERNODE_EDGE_PROPERTIES_BIN);
            Assert.assertTrue(supernodePMap.containsKey(v1.id.getKeyHashString()));
            Assert.assertTrue(supernodePMap.containsKey(v2.id.getKeyHashString()));
            Assert.assertEquals(4, supernodePMap.get(v1.id.getKeyHashString()).size());
            Assert.assertEquals(4, supernodePMap.get(v2.id.getKeyHashString()).size());
            Assert.assertEquals(2, supernodePMap.get(v1.id.getKeyHashString()).get("foo").size());
            Assert.assertEquals(2, supernodePMap.get(v1.id.getKeyHashString()).get("~label").size());
            Assert.assertEquals(1, supernodePMap.get(v1.id.getKeyHashString()).get("culprit").size());
            Assert.assertEquals(2, supernodePMap.get(v2.id.getKeyHashString()).get("foo").size());
            Assert.assertEquals(2, supernodePMap.get(v2.id.getKeyHashString()).get("~label").size());
            Assert.assertEquals(1, supernodePMap.get(v2.id.getKeyHashString()).get("culprit").size());

            // See that removal on the original e1 handle deletes the "culprit" property
            e1.remove();
            record = v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, Collections.emptyList()).next().record;
            supernodePMap = (Map<String, Map<String, Map<Long, Object>>>) record.getMap(phatFirefly.getBaseGraph().SUPERNODE_EDGE_PROPERTIES_BIN);
            Assert.assertTrue(supernodePMap.containsKey(v1.id.getKeyHashString()));
            Assert.assertTrue(supernodePMap.containsKey(v2.id.getKeyHashString()));
            Assert.assertEquals(3, supernodePMap.get(v1.id.getKeyHashString()).size());
            Assert.assertEquals(3, supernodePMap.get(v2.id.getKeyHashString()).size());
            Assert.assertEquals(1, supernodePMap.get(v1.id.getKeyHashString()).get("foo").size());
            Assert.assertEquals(1, supernodePMap.get(v1.id.getKeyHashString()).get("~label").size());
            Assert.assertFalse(supernodePMap.get(v1.id.getKeyHashString()).containsKey("culprit"));
            Assert.assertEquals(1, supernodePMap.get(v2.id.getKeyHashString()).get("foo").size());
            Assert.assertEquals(1, supernodePMap.get(v2.id.getKeyHashString()).get("~label").size());
            Assert.assertFalse(supernodePMap.get(v2.id.getKeyHashString()).containsKey("culprit"));
        }
    }

    @Test
    public void testContainsWithinPushdown() {
        GraphTraversalSource g = graph.traversal();
        g.addE("e1").property("foo", "Simon").from(v1).to(v2).iterate();
        g.addE("e2").property("foo", "Lyndon").from(v1).to(v2).iterate();
        g.addE("e3").property("foo", 100).from(v1).to(v2).iterate();
        g.addE("e4").property("foo", 200).from(v1).to(v2).iterate();
        g.addE("e5").property("foo", "Valentyn").property("bar", 200).from(v1).to(v2).iterate();
        g.addE("e6").property("foo", 100).property("bar", "Lyndon").from(v1).to(v2).iterate();

        HasContainer hasFooSimon100 = new HasContainer("foo", P.within("Simon", 100));
        HasContainer hasBarLyndon = new HasContainer("bar", P.within("Lyndon"));
        HasContainer hasFooValentyn200Ishaan = new HasContainer("foo", P.within("Valentyn", 200, "Ishaan"));
        HasContainer compoundWithinHasBarLyndon = new HasContainer("foo", P.within(100, 200));
        HasContainer compoundEqHasBarLyndon = new HasContainer("foo", P.eq(100));
        HasContainer compoundNomatchHasBarLyndon = new HasContainer("foo", P.within(200, "Simon"));

        final FireflyVertex v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        Assert.assertEquals(3, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasFooSimon100))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBarLyndon))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasFooValentyn200Ishaan))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBarLyndon, compoundWithinHasBarLyndon))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBarLyndon, compoundEqHasBarLyndon))));
        Assert.assertEquals(0, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBarLyndon, compoundNomatchHasBarLyndon))));

        Assert.assertEquals(3, Iterators.size(g.V(v1.id()).outE().has("foo", P.within("Simon", 100))));
        Assert.assertEquals(1, Iterators.size(g.V(v1.id()).outE().has("bar", P.within("Lyndon"))));
        Assert.assertEquals(2, Iterators.size(g.V(v1.id()).outE().has("foo", P.within("Valentyn", "Ishaan", 200))));
        Assert.assertEquals(1, Iterators.size(g.V(v1.id()).outE().has("bar", P.within("Lyndon")).has("foo", P.within(100, 200))));
        Assert.assertEquals(1, Iterators.size(g.V(v1.id()).outE().has("bar", P.within("Lyndon")).has("foo", 100)));
        Assert.assertEquals(0, Iterators.size(g.V(v1.id()).outE().has("bar", P.within("Lyndon")).has("foo", P.within(200, "Simon"))));
    }
}
