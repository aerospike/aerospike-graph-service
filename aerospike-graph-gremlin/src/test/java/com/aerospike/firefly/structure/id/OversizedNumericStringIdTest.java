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

package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.GLOBAL_EDGE_CACHE_ENABLED;

public class OversizedNumericStringIdTest extends AbstractFireflySuite {
    private static final String OUT_OF_RANGE_NUMERIC_ID = "99999999999999999999";
    private static final String BITCOIN_ID = "1997231212526362784799127961188217";
    private static final String LONG_MAX_VALUE_PLUS_ONE_ID = "9223372036854775808";
    private static final long SMALL_NUMERIC_ID = 7L;

    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void testOutOfRangeNumericStringIdIsKeptAsString() {
        final FireflyId id = graph.getIdFactory().createVertexId(OUT_OF_RANGE_NUMERIC_ID);
        Assert.assertEquals(OUT_OF_RANGE_NUMERIC_ID, id.getStorageId());
        Assert.assertEquals(OUT_OF_RANGE_NUMERIC_ID, id.getUserId());

        final FireflyId negativeId = graph.getIdFactory().createVertexId("-" + OUT_OF_RANGE_NUMERIC_ID);
        Assert.assertEquals("-" + OUT_OF_RANGE_NUMERIC_ID, negativeId.getStorageId());

        final FireflyId positiveId = graph.getIdFactory().createVertexId("+" + OUT_OF_RANGE_NUMERIC_ID);
        Assert.assertEquals("+" + OUT_OF_RANGE_NUMERIC_ID, positiveId.getStorageId());

        final FireflyId dashId = graph.getIdFactory().createVertexId("-");
        Assert.assertEquals("-", dashId.getStorageId());

        final FireflyId plusId = graph.getIdFactory().createVertexId("+");
        Assert.assertEquals("+", plusId.getStorageId());
    }

    @Test
    public void testInRangeNumericStringIdIsStoredAsLongType() {
        final FireflyId id = graph.getIdFactory().createVertexId(String.valueOf(Long.MAX_VALUE));
        Assert.assertEquals(Long.MAX_VALUE, id.getStorageId());
        Assert.assertEquals(String.valueOf(Long.MAX_VALUE), id.getUserId());

        Assert.assertEquals(5L, graph.getIdFactory().createVertexId("+5").getStorageId());
        Assert.assertEquals(-5L, graph.getIdFactory().createVertexId("-5").getStorageId());
        Assert.assertEquals(Long.MIN_VALUE, graph.getIdFactory().createVertexId(String.valueOf(Long.MIN_VALUE)).getStorageId());
    }

    @Test
    public void testNumericStringAndNumericIdsResolveToSameVertex() {
        final GraphTraversalSource g = graph.traversal();
        g.addV("foo").property(T.id, "42").iterate();
        g.addV("bar").property(T.id, 7L).iterate();

        Assert.assertEquals("42", g.V("42").id().next());
        Assert.assertEquals("42", g.V(42L).id().next());
        Assert.assertEquals("42", g.V(42).id().next());
        Assert.assertEquals(7L, g.V(7L).id().next());
        Assert.assertEquals(7L, g.V("7").id().next());
        Assert.assertEquals(7L, g.V(7).id().next());
        Assert.assertEquals(7L, g.V("+7").id().next());

        Assert.assertThrows(IllegalArgumentException.class, () -> g.addV("baz").property(T.id, 42L).iterate());
        Assert.assertThrows(IllegalArgumentException.class, () -> g.addV("baz").property(T.id, "7").iterate());
        Assert.assertThrows(IllegalArgumentException.class, () -> g.addV("baz").property(T.id, "+7").iterate());
        Assert.assertEquals(2L, (long) g.V().count().next());
    }

    @Test
    public void testOutOfRangeNumericStringIdRoundTrip() {
        final GraphTraversalSource g = graph.traversal();
        g.addV("foo").property(T.id, OUT_OF_RANGE_NUMERIC_ID).iterate();
        g.addV("bar").property(T.id, "1").iterate();
        g.V(OUT_OF_RANGE_NUMERIC_ID).addE("baz").to(__.V("1")).iterate();

        final Vertex foo = g.V(OUT_OF_RANGE_NUMERIC_ID).next();
        Assert.assertEquals(OUT_OF_RANGE_NUMERIC_ID, foo.id());
        Assert.assertEquals(OUT_OF_RANGE_NUMERIC_ID, g.V("1").in("baz").id().next());
    }

    @Test
    public void testTraversalsWithOutOfRangeNumericStringIds() {
        loadAddressGraph(graph.traversal());
        assertAddressGraphTraversals(graph.traversal());
    }

    @Test
    public void testTraversalsWithOutOfRangeNumericStringIdsWithoutEdgeCache() {
        final Configuration noCacheConfig = ConfigurationUtils.cloneConfiguration(config);
        noCacheConfig.setProperty(GLOBAL_EDGE_CACHE_ENABLED.toLowerCase(), "false");
        noCacheConfig.setProperty(ConfigurationHelper.Keys.GRAPH_ID.toLowerCase(), "ncg");
        noCacheConfig.setProperty(Graph.GRAPH, "ncg");
        final FireflyGraph noCacheGraph = FireflyGraph.open(noCacheConfig);
        try {
            noCacheGraph.getBaseGraph().dropDatabase(noCacheGraph, false);
            loadAddressGraph(noCacheGraph.traversal());
            assertAddressGraphTraversals(noCacheGraph.traversal());
        } finally {
            noCacheGraph.getBaseGraph().dropDatabase(noCacheGraph, false);
            noCacheGraph.close();
        }
    }

    private static void loadAddressGraph(final GraphTraversalSource g) {
        g.addV("address").property(T.id, BITCOIN_ID).iterate();
        g.addV("address").property(T.id, LONG_MAX_VALUE_PLUS_ONE_ID).iterate();
        g.addV("address").property(T.id, SMALL_NUMERIC_ID).iterate();
        g.V(BITCOIN_ID).addE("sends").to(__.V(LONG_MAX_VALUE_PLUS_ONE_ID)).property("amount", 1L).iterate();
        g.V(BITCOIN_ID).addE("sends").to(__.V(SMALL_NUMERIC_ID)).property("amount", 2L).iterate();
        g.V(SMALL_NUMERIC_ID).addE("sends").to(__.V(BITCOIN_ID)).property("amount", 3L).iterate();
        g.V(LONG_MAX_VALUE_PLUS_ONE_ID).addE("sends").to(__.V(BITCOIN_ID)).property("amount", 4L).iterate();
    }

    private static void assertAddressGraphTraversals(final GraphTraversalSource g) {
        // Direct lookups.
        Assert.assertEquals(BITCOIN_ID, g.V(BITCOIN_ID).id().next());
        Assert.assertEquals(Set.of(BITCOIN_ID, LONG_MAX_VALUE_PLUS_ONE_ID, SMALL_NUMERIC_ID),
                Set.copyOf(g.V(BITCOIN_ID, LONG_MAX_VALUE_PLUS_ONE_ID, SMALL_NUMERIC_ID).id().toList()));
        Assert.assertEquals(BITCOIN_ID, g.V().hasId(BITCOIN_ID).id().next());
        Assert.assertEquals(1L, (long) g.V().has(T.id, BITCOIN_ID).count().next());
        Assert.assertEquals(2L, (long) g.V().hasId(P.within(BITCOIN_ID, LONG_MAX_VALUE_PLUS_ONE_ID)).count().next());
        Assert.assertEquals(Set.of(LONG_MAX_VALUE_PLUS_ONE_ID, SMALL_NUMERIC_ID),
                Set.copyOf(g.V().hasId(P.neq(BITCOIN_ID)).id().toList()));

        // Ids that are close to, but not, a stored id must not match.
        Assert.assertFalse(g.V(BITCOIN_ID.substring(0, BITCOIN_ID.length() - 1) + "8").hasNext());
        Assert.assertFalse(g.V(BITCOIN_ID.substring(0, 19)).hasNext());
        Assert.assertFalse(g.V(Long.MIN_VALUE).hasNext());
        Assert.assertFalse(g.V("9223372036854775809").hasNext());

        // Adjacency filtered by id.
        Assert.assertEquals(1L, (long) g.V(BITCOIN_ID).out("sends").hasId(LONG_MAX_VALUE_PLUS_ONE_ID).count().next());
        Assert.assertEquals(2L, (long) g.V(BITCOIN_ID).out("sends")
                .hasId(P.within(LONG_MAX_VALUE_PLUS_ONE_ID, SMALL_NUMERIC_ID)).count().next());
        Assert.assertEquals(1L, (long) g.V(BITCOIN_ID).in("sends").hasId(String.valueOf(SMALL_NUMERIC_ID)).count().next());
        Assert.assertEquals(1L, (long) g.V(BITCOIN_ID).outE("sends")
                .where(__.inV().hasId(LONG_MAX_VALUE_PLUS_ONE_ID)).values("amount").next());
        Assert.assertEquals(2L, (long) g.E().where(__.outV().hasId(BITCOIN_ID)).count().next());
        Assert.assertEquals(2L, (long) g.E().where(__.inV().hasId(BITCOIN_ID)).count().next());

        // Traversing to the vertex from its neighbors.
        Assert.assertEquals(BITCOIN_ID, g.V(SMALL_NUMERIC_ID).out("sends").id().next());
        Assert.assertEquals(BITCOIN_ID, g.V(LONG_MAX_VALUE_PLUS_ONE_ID).out("sends").id().next());
        Assert.assertEquals(Set.of(LONG_MAX_VALUE_PLUS_ONE_ID, SMALL_NUMERIC_ID),
                Set.copyOf(g.V(BITCOIN_ID).in("sends").id().toList()));
        Assert.assertEquals(2L, (long) g.V(BITCOIN_ID).both("sends").dedup().count().next());
        Assert.assertEquals(BITCOIN_ID, g.V(BITCOIN_ID).as("a").out("sends")
                .hasId(LONG_MAX_VALUE_PLUS_ONE_ID).select("a").id().next());
        Assert.assertEquals(List.of(BITCOIN_ID, LONG_MAX_VALUE_PLUS_ONE_ID, BITCOIN_ID),
                g.V(BITCOIN_ID).out("sends").hasId(LONG_MAX_VALUE_PLUS_ONE_ID).out("sends")
                        .path().by(T.id).next().objects());

        // Merges match the existing elements rather than creating new ones.
        Assert.assertEquals(BITCOIN_ID, g.mergeV(Map.of(T.id, BITCOIN_ID)).id().next());
        Assert.assertEquals(1L, (long) g.mergeE(Map.of(T.label, "sends",
                Direction.OUT, BITCOIN_ID, Direction.IN, LONG_MAX_VALUE_PLUS_ONE_ID)).values("amount").next());
        Assert.assertEquals(3L, (long) g.V().count().next());
        Assert.assertEquals(4L, (long) g.E().count().next());
    }
}
