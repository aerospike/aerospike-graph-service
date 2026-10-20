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

package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Test;

import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.GLOBAL_EDGE_CACHE_ENABLED;

public class FireflyCountGlobalLocalStrategyHasIdTest extends AbstractFireflySuite {
    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void testHasIdCountWithEdgeCache() {
        assertHasIdCounts(graph.traversal());
    }

    @Test
    public void testHasIdCountWithoutEdgeCache() {
        final Configuration noCacheConfig = ConfigurationUtils.cloneConfiguration(config);
        noCacheConfig.setProperty(GLOBAL_EDGE_CACHE_ENABLED.toLowerCase(), "false");
        noCacheConfig.setProperty(ConfigurationHelper.Keys.GRAPH_ID.toLowerCase(), "ncg");
        noCacheConfig.setProperty(Graph.GRAPH, "ncg");
        final FireflyGraph noCacheGraph = FireflyGraph.open(noCacheConfig);
        try {
            noCacheGraph.getBaseGraph().dropDatabase(noCacheGraph, false);
            assertHasIdCounts(noCacheGraph.traversal());
        } finally {
            noCacheGraph.getBaseGraph().dropDatabase(noCacheGraph, false);
            noCacheGraph.close();
        }
    }

    private static void assertHasIdCounts(final GraphTraversalSource g) {
        g.addV("a").property(T.id, 1L).iterate();
        g.addV("a").property(T.id, 2L).iterate();
        g.addV("a").property(T.id, "alice").iterate();
        g.V(1L).addE("e").to(__.V(2L)).iterate();
        g.V(1L).addE("e").to(__.V("alice")).iterate();
        g.V("alice").addE("e").to(__.V(1L)).iterate();
        final Object edgeId = g.V(1L).outE("e").where(__.inV().hasId(2L)).id().next();

        Assert.assertEquals(1L, (long) g.V(1L).out("e").hasId(2L).count().next());
        Assert.assertEquals(1L, (long) g.V(1L).out("e").hasId("alice").count().next());
        Assert.assertEquals(2L, (long) g.V(1L).out("e").hasId(P.within(2L, "alice")).count().next());
        Assert.assertEquals(0L, (long) g.V(1L).out("e").hasId(3L).count().next());
        Assert.assertEquals(1L, (long) g.V(1L).in("e").hasId("alice").count().next());
        Assert.assertEquals(2L, (long) g.V(1L).both("e").hasId("alice").count().next());
        Assert.assertEquals(1L, (long) g.V(1L).local(__.out("e").hasId(2L).count()).next());
        Assert.assertEquals(1L, (long) g.V(1L).outE("e").hasId(edgeId).count().next());
        Assert.assertEquals(1L, (long) g.V(1L).local(__.outE("e").hasId(edgeId).count()).next());
    }
}
