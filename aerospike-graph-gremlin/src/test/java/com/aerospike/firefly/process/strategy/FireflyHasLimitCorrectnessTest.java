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
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.AEROSPIKE_BATCH_READ_SIZE;
import static com.aerospike.firefly.util.config.ConfigurationHelper.Keys.MOVEMENT_BARRIER_SIZE;

public class FireflyHasLimitCorrectnessTest {

    @Test
    public void testHasLimitCorrectness() {
        final Configuration configuration = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        configuration.setProperty(AEROSPIKE_BATCH_READ_SIZE, 5);
        configuration.setProperty(MOVEMENT_BARRIER_SIZE, 50);
        try (final FireflyGraph graph = FireflyGraph.open(configuration)) {
            GraphTraversalSource g = graph.traversal();
            g.V().drop().iterate();
            for (int i = 0; i < 12000; i++) {
                g.addV().property(T.id, i).property("name", "vertex" + i).property("name2", 1.0).next();
            }

            for (int i = 0; i < 12000; i++) {
                g.addE("edge").from(__.V(i)).to(__.V((i + 1) % 1000)).property("weight", i).property("name2", 1.0).next();
            }
            GraphTraversal t1 = g.V().order().by(T.id).out().limit(1).has("name", "vertex500").has("name2", 1.0);
            GraphTraversal t2 = g.V().order().by(T.id).out().has("name", "vertex500").has("name2", 1.0).limit(1);

            t1.asAdmin().applyStrategies();
            t2.asAdmin().applyStrategies();

            List<Vertex> vs1 = g.V().order().by(T.id).out().limit(1).has("name", "vertex500").has("name2", 1.0).toList();
            List<Vertex> vs2 = g.V().order().by(T.id).out().has("name", "vertex500").limit(1).toList();
            List<Vertex> vs3 = g.V().order().by(T.id).out().has("name", "vertex500").limit(1).has("name2", 1.0).toList();
            List<Vertex> vs4 = g.V().order().by(T.id).out().has("name", "vertex500").limit(50).toList();

            List<Object> vsId1 = g.V().order().by(T.id).out().limit(1).has("name", "vertex500").has("name2", 1.0).id().toList();
            List<Object> vsId2 = g.V().order().by(T.id).out().has("name", "vertex500").id().limit(1).toList();
            List<Object> vsId3 = g.V().order().by(T.id).out().has("name", "vertex500").limit(1).has("name2", 1.0).id().toList();
            List<Object> vsId4 = g.V().order().by(T.id).out().id().limit(50).toList();

            List<Edge> es1 = g.V().order().by(T.id).outE().limit(1).has("weight", 500).has("name2", 1.0).toList();
            List<Edge> es2 = g.V().order().by(T.id).outE().has("weight", 500).limit(1).has("name2", 1.0).toList();

            Assert.assertTrue(vs1.isEmpty() && vs2.size() == 1 && vs2.get(0).id().equals(500) && vs3.size() == 1 && vs3.get(0).id().equals(500));
            Assert.assertTrue(es1.isEmpty() && es2.size() == 1);
            Assert.assertTrue(vsId1.isEmpty() && vsId2.size() == 1 && vsId2.get(0).equals(500) && vsId3.size() == 1 && vsId3.get(0).equals(500));
            Assert.assertTrue(vsId4.size() == 50);

        }

    }
}
