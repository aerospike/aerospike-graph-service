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

package com.aerospike.firefly.structure.util;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class FireflyAerospikeGraphServiceCheckTest {
    // This test should only be called with a different feature-key file that does not include the graph-service.
    // in the standard test suite.
    // mvn test -pl aerospike-graph-gremlin -Dtest=FireflyAerospikeGraphServiceCheckTest  -Dintegration.test.properties=packed --no-transfer-progress

    @Test
    public void testConnectWithoutGraphFeature() {
        final Configuration conf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        try (final FireflyGraph graph = FireflyGraph.open(conf)) {
            final GraphTraversalSource g = graph.traversal();
            final Vertex v1 = g.addV().next();
            final Vertex v2 = g.addV().next();
            g.addE("edge").from(v1).to(v2).next();
            g.addE("edge").from(v2).to(v1).next();
            Assert.assertEquals(2, g.V().toList().size());
            Assert.assertEquals(2, g.E().toList().size());

        }
    }
}
