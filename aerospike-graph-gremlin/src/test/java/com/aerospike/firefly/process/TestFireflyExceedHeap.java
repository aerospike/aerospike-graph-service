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

package com.aerospike.firefly.process;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

public class TestFireflyExceedHeap extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    @Ignore
    @Test
    public void testSmallMemoryOverload() {
        graph.close();

        config.setProperty(ConfigurationHelper.Keys.ENABLE_READ_THROUGH_CACHE.toLowerCase(), "false");
        config.setProperty(ConfigurationHelper.Keys.ENABLE_PREFETCH_STRATEGY.toLowerCase(), "false");

        db = AerospikeConnection.connect(config);
        graph = FireflyGraph.open(config);

        final GraphTraversalSource g = graph.traversal();

        // Must set -Djvmheapsize=small when calling maven. For example:
        // mvn test -pl aerospike-graph-gremlin -Dtest=TestFireflyExceedHeap  -Dintegration.test.properties=packed -Djvmheapsize=small --no-transfer-progress
        long heapSize = Runtime.getRuntime().maxMemory();
        long heapSizeMB = heapSize / 1024 / 1024;
        Assert.assertTrue(heapSizeMB < 1020);
        Assert.assertTrue(heapSizeMB > 980);


        // 1000000 vertices @ 16 bytes per entry * 1000 entries = 16 kB per vertex = 16 GB total.
        for (int i = 0; i < 100000; i++) {
            if (i != 0 && ((i + 1) % 250) == 0) {
                System.out.println("Adding vertex " + (i + 1));
            }
            GraphTraversal t = g.addV("person");
            for (int j = 0; j < 100; j++) {
                t = t.property(String.valueOf(j), j);
            }
            t.next();
        }

        // With vertex batch reading iterator, we should only materialize 5000 @ 50 kB each = 250 MB.
        // This should not exceed the heap size of 1 GB.
        g.V().count().next();
    }
}
