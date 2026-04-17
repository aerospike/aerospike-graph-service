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

package com.aerospike.firefly.phantomEdges;

import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.driver.remote.DriverRemoteConnection;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Test;

import static org.apache.tinkerpop.gremlin.process.traversal.AnonymousTraversalSource.traversal;

//@todo: move to bulk loader maven module
public class TestPhantomEdges {

    // This test expects that an aerospike-graph docker instance is running in GCP and a bulk load has completed
    // where one or more of the works was killed mid job. This will leave phantom edges in the graph which we will find.

    @Test
    public void findAllEdges() throws Exception {
        final DriverRemoteConnection driverRemoteConnection = DriverRemoteConnection.using("localhost", 8182, "g");
        final GraphTraversalSource g = traversal().withRemote(driverRemoteConnection);

        // Get total edges, edges from the left, and edges from the right.
        final long outECount = g.with(ConfigurationHelper.TraversalOptions.SCAN_QUERY_ENABLED).with("evaluationTimeout", 30 * 60 * 1000).V().outE().count().next();
        final long inECount = g.with(ConfigurationHelper.TraversalOptions.SCAN_QUERY_ENABLED).with("evaluationTimeout", 30 * 60 * 1000).V().inE().count().next();
        final long eCount = g.with(ConfigurationHelper.TraversalOptions.SCAN_QUERY_ENABLED).with("evaluationTimeout", 30 * 60 * 1000).E().count().next();

        // Compare to expected value.
        Assert.assertEquals(14000000L, eCount);
        Assert.assertEquals(14000000L, inECount);
        Assert.assertEquals(14000000L, outECount);
        driverRemoteConnection.close();
    }
}
