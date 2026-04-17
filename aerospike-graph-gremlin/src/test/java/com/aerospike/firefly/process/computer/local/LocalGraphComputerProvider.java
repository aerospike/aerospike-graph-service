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

package com.aerospike.firefly.process.computer.local;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyGraphProvider;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.GraphProvider;
import org.apache.tinkerpop.gremlin.TestHelper;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

@GraphProvider.Descriptor(computer = LocalGraphComputer.class)
public class LocalGraphComputerProvider extends FireflyGraphProvider {

    private static final Logger LOG = LoggerFactory.getLogger(LocalGraphComputerProvider.class);

    private FireflyGraph TEST_GRAPH; // don't close database, simply drop database between tests
    private static final Random RANDOM = TestHelper.RANDOM;

    @Override
    public Graph openTestGraph(final Configuration config) {
        if (null == TEST_GRAPH) {
            BaseConfiguration copy = new BaseConfiguration();
            copy.copy(config);
            copy.setProperty(ConfigurationHelper.Keys.SUMMARY_ENABLED_FLAG,true);
            TEST_GRAPH = (FireflyGraph) GraphFactory.open(copy);
        }
        return TEST_GRAPH;
    }

    @Override
    public void clear(final Graph graph, final Configuration configuration) {
        if (TEST_GRAPH == null || TEST_GRAPH.closed.get())
            TEST_GRAPH = FireflyGraph.open(configuration);
        TEST_GRAPH.getBaseGraph().dropDatabase(TEST_GRAPH, false);
        System.gc();
    }

    @Override
    public GraphTraversalSource traversal(final Graph graph) {
        return graph.traversal().withStrategies(
                VertexProgramStrategy.build()
                        .workers(RANDOM.nextInt(3) + 1)            // number of parallel threads
                        .graphComputer(RANDOM.nextBoolean() ?      // verifying semantics of api
                                GraphComputer.class :
                                LocalGraphComputer.class).create());
    }
}
