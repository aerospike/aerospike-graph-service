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
import com.aerospike.firefly.util.exceptions.GraphError;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.apache.tinkerpop.gremlin.process.traversal.Merge.onCreate;
import static org.apache.tinkerpop.gremlin.process.traversal.Merge.onMatch;

public class FireflyMergeEdgeStepNSupDisabledTest {
    @Test
    public void testNsupDisabledErrorMessage() {
        try (final FireflyGraph graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            try {
                graph.traversal().V().drop().iterate();
                graph.traversal().addV().property(T.id, "vertex1").iterate();
                graph.traversal().addV().property(T.id, "vertex2").iterate();
                graph.traversal().mergeE(Map.of(
                                T.label, "connected",
                                Direction.OUT, "vertex1",
                                Direction.IN, "vertex2"))
                        .option(onCreate, Map.of("state", "created"))
                        .option(onMatch, Map.of("state", "matched"))
                        .toList();
                Assert.fail("Should have thrown an exception for disabled mergeE queries.");
            } catch (Exception e) {
                Assert.assertEquals(e.getMessage(), GraphError.getMessage(GraphError.NSUP_DISABLED));
            }
        }
    }
}
