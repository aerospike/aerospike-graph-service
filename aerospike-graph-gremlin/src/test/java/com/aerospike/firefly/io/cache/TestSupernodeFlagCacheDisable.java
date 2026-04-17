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

package com.aerospike.firefly.io.cache;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.firefly.io.FireflyRecord;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Test;

public class TestSupernodeFlagCacheDisable extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void testSupernodeFlagSetAfterVertexCreation() {
        final GraphTraversalSource g = graph.traversal();

        // Create vertex.
        FireflyVertex v = (FireflyVertex) g.addV("test").next();

        // Validate vertex supernode flag is not set.
        final Key key = FireflyRecord.getKey(graph.getBaseGraph(), graph.getBaseGraph().getConfig().vertexAeroSet, v.id);
        Record r = graph.getBaseGraph().read(key, null);
        Assert.assertFalse(v.isEdgeCacheOverflowed());
        Assert.assertFalse(r.getBoolean(db.getConfig().edgeCacheDisabledBin));

        // Set supernode flag and grab vertex.
        v = (FireflyVertex) g.V(v.id()).property("~supernode", true).next();

        // Validate cache overflowed flag is set.
        Assert.assertTrue(v.isEdgeCacheOverflowed());
        r = graph.getBaseGraph().read(key, null);
        Assert.assertTrue(r.getBoolean(db.getConfig().edgeCacheDisabledBin));

        // Grab vertex through id.
        v = (FireflyVertex) g.V(v.id()).next();

        // Validate cache overflowed flag is set.
        Assert.assertTrue(v.isEdgeCacheOverflowed());
        r = graph.getBaseGraph().read(key, null);
        Assert.assertTrue(r.getBoolean(db.getConfig().edgeCacheDisabledBin));
    }

    @Test
    public void testSupernodeFlagSetOnVertexCreation() {
        final GraphTraversalSource g = graph.traversal();

        // Create vertex.
        FireflyVertex v = (FireflyVertex) g.addV("test").property("~supernode", true).next();

        // Validate vertex supernode flag is set.
        final Key key = FireflyRecord.getKey(graph.getBaseGraph(), graph.getBaseGraph().getConfig().vertexAeroSet, v.id);
        Record r = graph.getBaseGraph().read(key, null);
        Assert.assertTrue(v.isEdgeCacheOverflowed());
        Assert.assertTrue(r.getBoolean(db.getConfig().edgeCacheDisabledBin));

        v = (FireflyVertex) g.V().hasLabel("test").next();

        // Validate vertex supernode flag is set.
        r = graph.getBaseGraph().read(key, null);
        Assert.assertTrue(v.isEdgeCacheOverflowed());
        Assert.assertTrue(r.getBoolean(db.getConfig().edgeCacheDisabledBin));
    }
}
