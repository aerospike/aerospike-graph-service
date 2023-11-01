package com.aerospike.firefly.write;

import com.aerospike.firefly.io.impl.relational.packed.PackedVertex;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertexProperty;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.util.ConfigurationHelper;
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
import java.util.TreeMap;

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

            a.writeEdge(Direction.IN, fireflyGraph.getIdFactory().createId(getBytesId(1), FireflyEdge.class), "fail");
            b.writeEdge(Direction.OUT, fireflyGraph.getIdFactory().createId(getBytesId(1), FireflyEdge.class), "fail");
            Iterator<Edge> aOut = a.edges(Direction.OUT);
            Iterator<Edge> aIn = a.edges(Direction.IN);
            Iterator<Edge> bOut = b.edges(Direction.OUT);
            Iterator<Edge> bIn = b.edges(Direction.IN);
            Assert.assertFalse(aOut.hasNext());
            Assert.assertFalse(aIn.hasNext());
            Assert.assertFalse(bOut.hasNext());
            Assert.assertFalse(bIn.hasNext());

            FireflyEdge.writeEdge(fireflyGraph, fireflyGraph.getIdFactory().createId(getBytesId(1), FireflyEdge.class),
                    "fail", new ArrayList<>(), a, b, true, true);
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

            a.writeEdge(Direction.IN, fireflyGraph.getIdFactory().createId(getBytesId(1), FireflyEdge.class), "fail");
            b.writeEdge(Direction.OUT, fireflyGraph.getIdFactory().createId(getBytesId(1), FireflyEdge.class), "fail");
            FireflyEdge edge = FireflyEdge.writeEdge(fireflyGraph,
                    fireflyGraph.getIdFactory().createId(getBytesId(1), FireflyEdge.class), "fail", new ArrayList<>(), a, b ,
                    true, true);
            Iterator<Edge> aOut = a.edges(Direction.OUT);
            Iterator<Edge> aIn = a.edges(Direction.IN);
            Iterator<Edge> bOut = b.edges(Direction.OUT);
            Iterator<Edge> bIn = b.edges(Direction.IN);
            Assert.assertFalse(aOut.hasNext());
            Assert.assertTrue(aIn.hasNext());
            Assert.assertTrue(bOut.hasNext());
            Assert.assertFalse(bIn.hasNext());

            edge.removeEdge();
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
    public void writeVertexPropertyFailure() {
        // Test that if we partially write a vertex property, it does not show up half way.
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            Assume.assumeTrue(fireflyGraph.getDataModel().equals(FireflyGraph.getDataModelName()));
            fireflyGraph.getBaseGraph().dropDatabase(fireflyGraph, false);
            GraphTraversalSource g = fireflyGraph.traversal();

            FireflyVertex a = (FireflyVertex) g.addV().next();
            FireflyId id = fireflyGraph.getIdFactory().createId(1, FireflyVertexProperty.class);

            final FireflyVertexProperty fireflyVertexProperty = new PackedVertexProperty<>(fireflyGraph, id, (PackedVertex) a, "key", "value", new TreeMap<>(), new TreeMap<>());
            List<Object> properties = g.V().values("key").toList();
            Assert.assertTrue(properties.isEmpty());

            a.writeVertexProperty(fireflyVertexProperty);

            properties = g.V().values("key").toList();
            Assert.assertFalse(properties.isEmpty());
            Assert.assertEquals("value", properties.get(0));
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
            a.removeVertexProperty("key", vp.id);

            properties = g.V().values("key").toList();
            Assert.assertTrue(properties.isEmpty());
        }
    }
}
