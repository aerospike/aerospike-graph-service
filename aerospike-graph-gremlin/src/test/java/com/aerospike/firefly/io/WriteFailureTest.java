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

package com.aerospike.firefly.io;

import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class WriteFailureTest {
    @Before
    public void setup() {
    }

    private static byte[] getBytesId(final long value) {
        final ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.putLong(value);
        final byte[] id = new byte[16];
        System.arraycopy(buffer.array(), 0, id, 0, 8);
        return id;
    }

    @Test
    public void writeEdgeFailureTest() {
        // Test that if we partially write an edge it does not show up half way.
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            fireflyGraph.getBaseGraph().dropDatabase(fireflyGraph, false);
            GraphTraversalSource g = fireflyGraph.traversal();
            FireflyVertex a = (FireflyVertex) g.addV().next();
            FireflyVertex b = (FireflyVertex) g.addV().next();

            fireflyGraph.getAerospikeOperations().writeEdgeToVertex(a, Direction.IN, fireflyGraph.getIdFactory().createEdgeId(getBytesId(1)), "fail", null);
            fireflyGraph.getAerospikeOperations().writeEdgeToVertex(b, Direction.OUT, fireflyGraph.getIdFactory().createEdgeId(getBytesId(1)), "fail", null);
            Iterator<Edge> aOut = a.edges(Direction.OUT);
            Iterator<Edge> aIn = a.edges(Direction.IN);
            Iterator<Edge> bOut = b.edges(Direction.OUT);
            Iterator<Edge> bIn = b.edges(Direction.IN);
            Assert.assertFalse(aOut.hasNext());
            Assert.assertFalse(aIn.hasNext());
            Assert.assertFalse(bOut.hasNext());
            Assert.assertFalse(bIn.hasNext());

            fireflyGraph.getAerospikeOperations().writeEdgeToRecord(fireflyGraph.getIdFactory().createEdgeId(getBytesId(1)),
                    "fail", new ArrayList<>(), a, b, true, true, null);
            aOut = a.edges(Direction.OUT);
            aIn = a.edges(Direction.IN);
            bOut = b.edges(Direction.OUT);
            bIn = b.edges(Direction.IN);
            Assert.assertFalse(aOut.hasNext());
            Assert.assertTrue(aIn.hasNext());
            Assert.assertTrue(bOut.hasNext());
            Assert.assertFalse(bIn.hasNext());
        }
    }

    @Test
    public void removeEdgeFailureTest() {
        // Test that if we partially remove an edge it does not show up half way.
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            fireflyGraph.getBaseGraph().dropDatabase(fireflyGraph, false);
            GraphTraversalSource g = fireflyGraph.traversal();
            FireflyVertex a = (FireflyVertex) g.addV().next();
            FireflyVertex b = (FireflyVertex) g.addV().next();

            fireflyGraph.getAerospikeOperations().writeEdgeToVertex(a, Direction.IN, fireflyGraph.getIdFactory().createEdgeId(getBytesId(1)), "fail", null);
            fireflyGraph.getAerospikeOperations().writeEdgeToVertex(b, Direction.OUT, fireflyGraph.getIdFactory().createEdgeId(getBytesId(1)), "fail", null);
            FireflyEdge edge = fireflyGraph.getAerospikeOperations().writeEdgeToRecord(fireflyGraph.getIdFactory().createEdgeId(getBytesId(1)),
                    "fail", new ArrayList<>(), a, b, true, true, null);
            Iterator<Edge> aOut = a.edges(Direction.OUT);
            Iterator<Edge> aIn = a.edges(Direction.IN);
            Iterator<Edge> bOut = b.edges(Direction.OUT);
            Iterator<Edge> bIn = b.edges(Direction.IN);
            Assert.assertFalse(aOut.hasNext());
            Assert.assertTrue(aIn.hasNext());
            Assert.assertTrue(bOut.hasNext());
            Assert.assertFalse(bIn.hasNext());

            fireflyGraph.getAerospikeOperations().removeEdge(edge);
            aOut = a.edges(Direction.OUT);
            aIn = a.edges(Direction.IN);
            bOut = b.edges(Direction.OUT);
            bIn = b.edges(Direction.IN);
            Assert.assertFalse(aOut.hasNext());
            Assert.assertFalse(aIn.hasNext());
            Assert.assertFalse(bOut.hasNext());
            Assert.assertFalse(bIn.hasNext());
        }
    }

    @Test
    public void removeVertexPropertyFailure() {
        // Test that if we partially remove a vertex property, it does not show up half way.
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            Assume.assumeTrue(fireflyGraph.getDataModel().equals(FireflyGraph.getDataModelName()));
            fireflyGraph.getBaseGraph().dropDatabase(fireflyGraph, false);
            GraphTraversalSource g = fireflyGraph.traversal();

            FireflyVertex a = (FireflyVertex) g.addV().next();
            a.property("key", "value");

            List<Object> properties = g.V().values("key").toList();
            Assert.assertFalse(properties.isEmpty());
            Assert.assertEquals("value", properties.get(0));

            final FireflyVertexProperty vp = (FireflyVertexProperty) a.property("key");
            fireflyGraph.getAerospikeOperations().removeVertexProperty(a, "key", "value", vp.id);

            properties = g.V().values("key").toList();
            Assert.assertTrue(properties.isEmpty());
        }
    }
}
