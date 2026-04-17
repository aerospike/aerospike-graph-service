/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_LABEL_KEY;
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
        graph.getBaseGraph().dropDatabase(graph, false);
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
        g.addE("e1").property("foo", "Bob").from(v1).to(v2).iterate();
        g.addE("e2").property("foo", "Alice").from(v1).to(v2).iterate();
        g.addE("e3").property("foo", 100).from(v1).to(v2).iterate();
        g.addE("e4").property("foo", 100).from(v1).to(v2).iterate();
        g.addE("e5").property("foo", 200).from(v1).to(v2).iterate();
        g.addE("e6").property("bar", "Alice").from(v1).to(v2).iterate();
        g.addE("e7").property("foo", 100).property("bar", "Alice").from(v1).to(v2).iterate();
        String e7id = (String) g.E().hasLabel("e7").next().id();
        String e8id = (String) g.addE("e8").from(v1).to(v2).next().id();
        g.E().hasLabel("e8").drop().iterate();

        HasContainer hasBob = new HasContainer("foo", P.eq("Bob"));
        HasContainer hasAlice = new HasContainer("bar", P.eq("Alice"));
        HasContainer has100 = new HasContainer("foo", P.eq(100));
        HasContainer hasId = new HasContainer(T.id.getAccessor(), P.eq(e7id));
        HasContainer hasIdFalse = new HasContainer(T.id.getAccessor(), P.eq(e8id));

        FireflyVertex v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(3, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasAlice))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasId))));
        Assert.assertEquals(0, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasIdFalse))));

        FireflyVertex v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(3, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasAlice))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasId))));
        Assert.assertEquals(0, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasIdFalse))));
    }

    @Test
    public void testPropertyAndLabelPushdown() {
        GraphTraversalSource g = graph.traversal();
        g.addE("e1").property("foo", "Bob").from(v1).to(v2).iterate();
        g.addE("e2").property("foo", "Alice").from(v1).to(v2).iterate();
        g.addE("e3").property("foo", 100).from(v1).to(v2).iterate();
        g.addE("e4").property("foo", 100).from(v1).to(v2).iterate();
        g.addE("e5").property("foo", 200).from(v1).to(v2).iterate();
        g.addE("e6").property("bar", "Alice").from(v1).to(v2).iterate();
        g.addE("e7").property("foo", 100).property("bar", "Alice").from(v1).to(v2).iterate();

        HasContainer hasBob = new HasContainer("foo", P.eq("Bob"));
        HasContainer hasAlice = new HasContainer("bar", P.eq("Alice"));
        HasContainer has100 = new HasContainer("foo", P.eq(100));

        FireflyVertex v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e1", "e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(0, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(3, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e3", "e4", "e7", "e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e3", "e7"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e7", "e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasAlice))));
        Assert.assertEquals(0, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e3", "e4", "e6"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e7"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e7", "e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));

        FireflyVertex v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e1", "e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(0, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(3, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e3", "e4", "e7", "e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e3", "e7"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e7", "e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasAlice))));
        Assert.assertEquals(0, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e3", "e4", "e6"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e7"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e7", "e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));
    }

    @Test
    public void testPropertyAndLabelPushdownPropertyAddedAfterCreation() {
        GraphTraversalSource g = graph.traversal();
        g.addE("e1").from(v1).to(v2).iterate();
        g.E().hasLabel("e1").property("foo", "Bob").iterate();
        g.addE("e2").from(v1).to(v2).iterate();
        g.E().hasLabel("e2").property("foo", "Alice").iterate();
        g.addE("e3").from(v1).to(v2).iterate();
        g.E().hasLabel("e3").property("foo", 100).iterate();
        g.addE("e4").from(v1).to(v2).iterate();
        g.E().hasLabel("e4").property("foo", 100).iterate();
        g.addE("e5").from(v1).to(v2).iterate();
        g.E().hasLabel("e5").property("foo", 200).iterate();
        g.addE("e6").from(v1).to(v2).iterate();
        g.E().hasLabel("e6").property("bar", "Alice").iterate();
        g.addE("e7").from(v1).to(v2).iterate();
        g.E().hasLabel("e7").property("foo", 100).property("bar", "Alice").iterate();

        HasContainer hasBob = new HasContainer("foo", P.eq("Bob"));
        HasContainer hasAlice = new HasContainer("bar", P.eq("Alice"));
        HasContainer has100 = new HasContainer("foo", P.eq(100));

        FireflyVertex v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e1", "e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(0, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(3, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e3", "e4", "e7", "e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e3", "e7"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e7", "e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasAlice))));
        Assert.assertEquals(0, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e3", "e4", "e6"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e7"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Set.of("e7", "e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));

        FireflyVertex v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e1", "e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(0, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(3, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e3", "e4", "e7", "e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e3", "e7"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e7", "e2"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasAlice))));
        Assert.assertEquals(0, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e3", "e4", "e6"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e7"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Set.of("e7", "e1"), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));
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
    public void testPropertyRemovePushdown() {
        GraphTraversalSource g = graph.traversal();
        g.addE("e1").property("foo", "Bob").from(v1).to(v2).iterate();
        g.addE("e2").property("foo", "Alice").from(v1).to(v2).iterate();
        g.addE("e3").property("foo", 100).from(v1).to(v2).iterate();
        g.addE("e4").property("foo", 100).from(v1).to(v2).iterate();
        g.E().hasLabel("e4").properties("foo").drop().iterate();
        g.addE("e5").property("foo", 200).from(v1).to(v2).iterate();
        g.addE("e6").property("bar", "Alice").from(v1).to(v2).iterate();
        g.addE("e7").property("foo", 100).property("bar", "Alice").from(v1).to(v2).iterate();

        HasContainer hasBob = new HasContainer("foo", P.eq("Bob"));
        HasContainer hasAlice = new HasContainer("bar", P.eq("Alice"));
        HasContainer has100 = new HasContainer("foo", P.eq(100));

        FireflyVertex v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasAlice))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));

        FireflyVertex v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasAlice))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));
    }

    @Test
    public void testPropertyUpdatePushdown() {
        GraphTraversalSource g = graph.traversal();
        g.addE("e1").property("foo", "Bob").from(v1).to(v2).iterate();
        g.addE("e2").property("foo", "Alice").from(v1).to(v2).iterate();
        g.addE("e3").property("foo", 100).from(v1).to(v2).iterate();
        g.addE("e4").property("foo", 100).from(v1).to(v2).iterate();
        g.E().hasLabel("e4").property("foo", 200).iterate();
        g.addE("e5").property("foo", 200).from(v1).to(v2).iterate();
        g.addE("e6").property("bar", "Alice").from(v1).to(v2).iterate();
        g.addE("e7").property("foo", 100).property("bar", "Alice").from(v1).to(v2).iterate();

        HasContainer hasBob = new HasContainer("foo", P.eq("Bob"));
        HasContainer hasAlice = new HasContainer("bar", P.eq("Alice"));
        HasContainer has100 = new HasContainer("foo", P.eq(100));
        HasContainer has200 = new HasContainer("foo", P.eq(200));

        FireflyVertex v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has200))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasAlice))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));

        FireflyVertex v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has200))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasAlice))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));
    }

    @Test
    public void testPropertyUpdatePushdownInvalidPushdown() {
        GraphTraversalSource g = graph.traversal();
        g.addE("e1").property("foo", "Bob").from(v1).to(v2).iterate();
        g.addE("e2").property("foo", "Alice").from(v1).to(v2).iterate();
        g.addE("e3").property("foo", 100).from(v1).to(v2).iterate();
        g.addE("e4").property("foo", 100).from(v1).to(v2).iterate();
        g.E().hasLabel("e4").property("foo", false).iterate();
        g.addE("e5").property("foo", 200).from(v1).to(v2).iterate();
        g.addE("e6").property("bar", "Alice").from(v1).to(v2).iterate();
        g.addE("e7").property("foo", 100).property("bar", "Alice").from(v1).to(v2).iterate();

        HasContainer hasBob = new HasContainer("foo", P.eq("Bob"));
        HasContainer hasAlice = new HasContainer("bar", P.eq("Alice"));
        HasContainer has100 = new HasContainer("foo", P.eq(100));
        HasContainer has200 = new HasContainer("foo", P.eq(200));

        FireflyVertex v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has200))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasAlice))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));

        FireflyVertex v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has200))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasAlice))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));
    }

    @Test
    public void testPropertyNullPushdown() {
        GraphTraversalSource g = graph.traversal();
        g.addE("e1").property("foo", "Bob").from(v1).to(v2).iterate();
        g.addE("e2").property("foo", "Alice").from(v1).to(v2).iterate();
        g.addE("e3").property("foo", 100).from(v1).to(v2).iterate();
        g.addE("e4").property("foo", 100).from(v1).to(v2).iterate();
        g.E().hasLabel("e4").property("foo", null).iterate();
        g.addE("e5").property("foo", 200).from(v1).to(v2).iterate();
        g.addE("e6").property("bar", "Alice").from(v1).to(v2).iterate();
        g.addE("e7").property("foo", 100).property("bar", "Alice").from(v1).to(v2).iterate();

        HasContainer hasBob = new HasContainer("foo", P.eq("Bob"));
        HasContainer hasAlice = new HasContainer("bar", P.eq("Alice"));
        HasContainer has100 = new HasContainer("foo", P.eq(100));

        FireflyVertex v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasAlice))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));

        FireflyVertex v2 = (FireflyVertex) g.V().hasLabel("v2").next();
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBob))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100))));
        Assert.assertEquals(2, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasAlice))));
        Assert.assertEquals(1, Iterators.size(v2.getEdgeKeyRecordsByIndex(Direction.IN, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(has100, hasAlice))));
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

            final Long labelPropertyKey = phatFirefly.getBaseGraph().schemaManager.getEdgePropertyRead(EDGE_SUPERNODE_LABEL_KEY);
            final Long fromtoPropertyKey = phatFirefly.getBaseGraph().schemaManager.getEdgePropertyRead("fromto");

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
            Map<Object, Map<Long, Map<Long, Object>>> supernodeInMap = (Map<Object, Map<Long, Map<Long, Object>>>) record.getMap(phatFirefly.getBaseGraph().getConfig().supernodesInBin);
            Map<Object, Map<Long, Map<Long, Object>>> supernodeOutMap = (Map<Object, Map<Long, Map<Long, Object>>>) record.getMap(phatFirefly.getBaseGraph().getConfig().supernodesOutBin);
            Assert.assertTrue(supernodeOutMap.containsKey(v1.id.getKeyHashString()));
            Assert.assertTrue(supernodeInMap.containsKey(v2.id.getKeyHashString()));
            Assert.assertTrue(supernodeOutMap.containsKey(v3.id.getKeyHashString()));
            Assert.assertTrue(supernodeInMap.containsKey(v4.id.getKeyHashString()));
            Assert.assertEquals(3, supernodeOutMap.get(v1.id.getKeyHashString()).size());
            Assert.assertEquals(3, supernodeInMap.get(v2.id.getKeyHashString()).size());
            Assert.assertEquals(3, supernodeOutMap.get(v3.id.getKeyHashString()).size());
            Assert.assertEquals(3, supernodeInMap.get(v4.id.getKeyHashString()).size());
            Assert.assertEquals(2, supernodeOutMap.get(v1.id.getKeyHashString()).get(fromtoPropertyKey).size());
            Assert.assertEquals(2, supernodeOutMap.get(v1.id.getKeyHashString()).get(labelPropertyKey).size());
            Assert.assertEquals(2, supernodeInMap.get(v2.id.getKeyHashString()).get(fromtoPropertyKey).size());
            Assert.assertEquals(2, supernodeInMap.get(v2.id.getKeyHashString()).get(labelPropertyKey).size());
            Assert.assertEquals(1, supernodeOutMap.get(v3.id.getKeyHashString()).get(fromtoPropertyKey).size());
            Assert.assertEquals(1, supernodeOutMap.get(v3.id.getKeyHashString()).get(labelPropertyKey).size());
            Assert.assertEquals(1, supernodeInMap.get(v4.id.getKeyHashString()).get(fromtoPropertyKey).size());
            Assert.assertEquals(1, supernodeInMap.get(v4.id.getKeyHashString()).get(labelPropertyKey).size());

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
            supernodeInMap = (Map<Object, Map<Long, Map<Long, Object>>>) record.getMap(phatFirefly.getBaseGraph().getConfig().supernodesInBin);
            supernodeOutMap = (Map<Object, Map<Long, Map<Long, Object>>>) record.getMap(phatFirefly.getBaseGraph().getConfig().supernodesOutBin);
            Assert.assertTrue(supernodeOutMap.containsKey(v1.id.getKeyHashString()));
            Assert.assertTrue(supernodeInMap.containsKey(v2.id.getKeyHashString()));
            Assert.assertTrue(supernodeOutMap.containsKey(v3.id.getKeyHashString()));
            Assert.assertTrue(supernodeInMap.containsKey(v4.id.getKeyHashString()));
            Assert.assertEquals(3, supernodeOutMap.get(v1.id.getKeyHashString()).size());
            Assert.assertEquals(3, supernodeInMap.get(v2.id.getKeyHashString()).size());
            Assert.assertEquals(3, supernodeOutMap.get(v3.id.getKeyHashString()).size());
            Assert.assertEquals(3, supernodeInMap.get(v4.id.getKeyHashString()).size());
            Assert.assertEquals(1, supernodeOutMap.get(v1.id.getKeyHashString()).get(fromtoPropertyKey).size());
            Assert.assertEquals(1, supernodeOutMap.get(v1.id.getKeyHashString()).get(labelPropertyKey).size());
            Assert.assertEquals(1, supernodeInMap.get(v2.id.getKeyHashString()).get(fromtoPropertyKey).size());
            Assert.assertEquals(1, supernodeInMap.get(v2.id.getKeyHashString()).get(labelPropertyKey).size());
            Assert.assertEquals(1, supernodeOutMap.get(v3.id.getKeyHashString()).get(fromtoPropertyKey).size());
            Assert.assertEquals(1, supernodeOutMap.get(v3.id.getKeyHashString()).get(labelPropertyKey).size());
            Assert.assertEquals(1, supernodeInMap.get(v4.id.getKeyHashString()).get(fromtoPropertyKey).size());
            Assert.assertEquals(1, supernodeInMap.get(v4.id.getKeyHashString()).get(labelPropertyKey).size());

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
            supernodeInMap = (Map<Object, Map<Long, Map<Long, Object>>>) record.getMap(phatFirefly.getBaseGraph().getConfig().supernodesInBin);
            supernodeOutMap = (Map<Object, Map<Long, Map<Long, Object>>>) record.getMap(phatFirefly.getBaseGraph().getConfig().supernodesOutBin);
            Assert.assertTrue(supernodeOutMap.containsKey(v1.id.getKeyHashString()));
            Assert.assertTrue(supernodeInMap.containsKey(v2.id.getKeyHashString()));
            Assert.assertFalse(supernodeOutMap.containsKey(v3.id.getKeyHashString()));
            Assert.assertFalse(supernodeInMap.containsKey(v4.id.getKeyHashString()));
            Assert.assertEquals(3, supernodeOutMap.get(v1.id.getKeyHashString()).size());
            Assert.assertEquals(3, supernodeInMap.get(v2.id.getKeyHashString()).size());
            Assert.assertEquals(1, supernodeOutMap.get(v1.id.getKeyHashString()).get(fromtoPropertyKey).size());
            Assert.assertEquals(1, supernodeOutMap.get(v1.id.getKeyHashString()).get(labelPropertyKey).size());
            Assert.assertEquals(1, supernodeInMap.get(v2.id.getKeyHashString()).get(fromtoPropertyKey).size());
            Assert.assertEquals(1, supernodeInMap.get(v2.id.getKeyHashString()).get(labelPropertyKey).size());
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

            final Long labelPropertyKey = phatFirefly.getBaseGraph().schemaManager.getEdgePropertyRead(EDGE_SUPERNODE_LABEL_KEY);
            final Long fooPropertyKey = phatFirefly.getBaseGraph().schemaManager.getEdgePropertyRead("foo");
            final Long culpritPropertyKey = phatFirefly.getBaseGraph().schemaManager.getEdgePropertyRead("culprit");

            Record record = v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, Collections.emptyList()).next().record;
            Map<Object, Map<Long, Map<Long, Object>>> supernodeOutMap = (Map<Object, Map<Long, Map<Long, Object>>>) record.getMap(phatFirefly.getBaseGraph().getConfig().supernodesOutBin);
            Map<Object, Map<Long, Map<Long, Object>>> supernodeInMap = (Map<Object, Map<Long, Map<Long, Object>>>) record.getMap(phatFirefly.getBaseGraph().getConfig().supernodesInBin);
            Assert.assertTrue(supernodeOutMap.containsKey(v1.id.getKeyHashString()));
            Assert.assertTrue(supernodeInMap.containsKey(v2.id.getKeyHashString()));
            Assert.assertEquals(4, supernodeOutMap.get(v1.id.getKeyHashString()).size());
            Assert.assertEquals(4, supernodeInMap.get(v2.id.getKeyHashString()).size());
            Assert.assertEquals(2, supernodeOutMap.get(v1.id.getKeyHashString()).get(fooPropertyKey).size());
            Assert.assertEquals(2, supernodeOutMap.get(v1.id.getKeyHashString()).get(labelPropertyKey).size());
            Assert.assertEquals(1, supernodeOutMap.get(v1.id.getKeyHashString()).get(culpritPropertyKey).size());
            Assert.assertEquals(2, supernodeInMap.get(v2.id.getKeyHashString()).get(fooPropertyKey).size());
            Assert.assertEquals(2, supernodeInMap.get(v2.id.getKeyHashString()).get(labelPropertyKey).size());
            Assert.assertEquals(1, supernodeInMap.get(v2.id.getKeyHashString()).get(culpritPropertyKey).size());

            // See that removal on the original e1 handle deletes the "culprit" property
            e1.remove();
            record = v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, Collections.emptyList()).next().record;
            supernodeOutMap = (Map<Object, Map<Long, Map<Long, Object>>>) record.getMap(phatFirefly.getBaseGraph().getConfig().supernodesOutBin);
            supernodeInMap = (Map<Object, Map<Long, Map<Long, Object>>>) record.getMap(phatFirefly.getBaseGraph().getConfig().supernodesInBin);
            Assert.assertTrue(supernodeOutMap.containsKey(v1.id.getKeyHashString()));
            Assert.assertTrue(supernodeInMap.containsKey(v2.id.getKeyHashString()));
            Assert.assertEquals(3, supernodeOutMap.get(v1.id.getKeyHashString()).size());
            Assert.assertEquals(3, supernodeInMap.get(v2.id.getKeyHashString()).size());
            Assert.assertEquals(1, supernodeOutMap.get(v1.id.getKeyHashString()).get(fooPropertyKey).size());
            Assert.assertEquals(1, supernodeOutMap.get(v1.id.getKeyHashString()).get(labelPropertyKey).size());
            Assert.assertFalse(supernodeOutMap.get(v1.id.getKeyHashString()).containsKey(culpritPropertyKey));
            Assert.assertEquals(1, supernodeInMap.get(v2.id.getKeyHashString()).get(fooPropertyKey).size());
            Assert.assertEquals(1, supernodeInMap.get(v2.id.getKeyHashString()).get(labelPropertyKey).size());
            Assert.assertFalse(supernodeInMap.get(v2.id.getKeyHashString()).containsKey(culpritPropertyKey));
        }
    }

    @Test
    public void testContainsWithinPushdown() {
        GraphTraversalSource g = graph.traversal();
        g.addE("e1").property("foo", "Bob").from(v1).to(v2).iterate();
        g.addE("e2").property("foo", "Alice").from(v1).to(v2).iterate();
        g.addE("e3").property("foo", 100).from(v1).to(v2).iterate();
        g.addE("e4").property("foo", 200).from(v1).to(v2).iterate();
        g.addE("e5").property("foo", "Valentyn").property("bar", 200).from(v1).to(v2).iterate();
        g.addE("e6").property("foo", 100).property("bar", "Alice").from(v1).to(v2).iterate();

        HasContainer hasFooBob100 = new HasContainer("foo", P.within("Bob", 100));
        HasContainer hasBarAlice = new HasContainer("bar", P.within("Alice"));
        HasContainer hasFooValentyn200Ishaan = new HasContainer("foo", P.within("Valentyn", 200, "Ishaan"));
        HasContainer compoundWithinHasBarAlice = new HasContainer("foo", P.within(100, 200));
        HasContainer compoundEqHasBarAlice = new HasContainer("foo", P.eq(100));
        HasContainer compoundNomatchHasBarAlice = new HasContainer("foo", P.within(200, "Bob"));

        final FireflyVertex v1 = (FireflyVertex) g.V().hasLabel("v1").next();
        Assert.assertEquals(3, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasFooBob100))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBarAlice))));
        Assert.assertEquals(2, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasFooValentyn200Ishaan))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBarAlice, compoundWithinHasBarAlice))));
        Assert.assertEquals(1, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBarAlice, compoundEqHasBarAlice))));
        Assert.assertEquals(0, Iterators.size(v1.getEdgeKeyRecordsByIndex(Direction.OUT, Collections.emptySet(), FireflyPhatEdgeIdIteratorFromVertex.OutputType.EDGE_ID, List.of(hasBarAlice, compoundNomatchHasBarAlice))));

        Assert.assertEquals(3, Iterators.size(g.V(v1.id()).outE().has("foo", P.within("Bob", 100))));
        Assert.assertEquals(1, Iterators.size(g.V(v1.id()).outE().has("bar", P.within("Alice"))));
        Assert.assertEquals(2, Iterators.size(g.V(v1.id()).outE().has("foo", P.within("Valentyn", "Ishaan", 200))));
        Assert.assertEquals(1, Iterators.size(g.V(v1.id()).outE().has("bar", P.within("Alice")).has("foo", P.within(100, 200))));
        Assert.assertEquals(1, Iterators.size(g.V(v1.id()).outE().has("bar", P.within("Alice")).has("foo", 100)));
        Assert.assertEquals(0, Iterators.size(g.V(v1.id()).outE().has("bar", P.within("Alice")).has("foo", P.within(200, "Bob"))));
    }
}
