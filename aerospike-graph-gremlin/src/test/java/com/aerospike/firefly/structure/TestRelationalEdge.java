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

package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestRelationalEdge {
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);

    private FireflyGraph graph;
    private Vertex from;
    private Vertex to;

    @Before
    public void beforeEach() {
        this.graph = FireflyGraph.open(CONFIG);
        this.graph.getBaseGraph().dropDatabase(graph, false);
        var g = graph.traversal();
        this.from = g.addV("from").next();
        this.to = g.addV("to").next();
    }

    @After
    public void after() {
        this.graph.getBaseGraph().dropDatabase(graph, false);
        this.graph.close();
    }

    @Test
    public void testPhatEdgeRemoval() {
        var g = graph.traversal();

        // Simple sanity test that writing and removing from phat edges doesn't fail
        final long totalEdgeCount = graph.getBaseGraph().getConfig().phatEdgeSize + 1;
        for (int i = 0; i < totalEdgeCount; i++) {
            g.addE(String.valueOf(i)).from(this.from).to(this.to).iterate();
        }
        Assert.assertEquals(totalEdgeCount, IteratorUtils.count(g.E()));
        for (int i = 0; i < totalEdgeCount; i++) {
            g.E().hasLabel(String.valueOf(i)).drop().iterate();
            Assert.assertEquals(totalEdgeCount - (i + 1), IteratorUtils.count(g.E()));
        }
        Assert.assertEquals(0, IteratorUtils.count(g.E()));

        // Check that removing the last edge from a phat edge record deletes the record and we can write to it after too
        g.addE("edge").from(this.from).to(this.to).iterate();
        Set<String> nonEmptySets = AerospikeConnection.InfoOps.getNonEmptySetList(graph.getBaseGraph());
        Assert.assertTrue(nonEmptySets.contains(graph.getBaseGraph().getConfig().edgeAeroSet));
        g.E().hasLabel("edge").drop().iterate();
        nonEmptySets = AerospikeConnection.InfoOps.getNonEmptySetList(graph.getBaseGraph());
        Assert.assertFalse(nonEmptySets.contains(graph.getBaseGraph().getConfig().edgeAeroSet));
        g.addE("edge").from(this.from).to(this.to).iterate();
        Assert.assertTrue(g.E().hasLabel("edge").hasNext());
    }
}
