package com.aerospike.firefly.io;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class WriteOverflowTest {
    // 1 kB string.
    private static final int STRING_LENGTH = 1000;
    private static final String CHARACTERS = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    private static final Random RANDOM = new Random();
    private static final int MAX_SIZE = 10 * 1000;
    private static final int PROPERTY_COUNT = 1000;
    private static final String RANDOM_STRING;
    static {
        final StringBuilder stringBuilder = new StringBuilder();
        while (stringBuilder.length() < STRING_LENGTH) {
            int idx = (int) (RANDOM.nextFloat() * CHARACTERS.length());
            stringBuilder.append(CHARACTERS.charAt(idx));
        }
        RANDOM_STRING = stringBuilder.toString();
    }

    @Before
    public void setup() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            Assume.assumeTrue(fireflyGraph.getDataModel().equals(FireflyGraph.getDataModelName()));
        }
    }

    @Test
    public void testWriteVertexPropertyOverflow() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            fireflyGraph.getBaseGraph().dropDatabase(fireflyGraph, false);

            final GraphTraversalSource g = fireflyGraph.traversal();
            Vertex vertex = g.addV("vertex_test").next();
            int i = 0;
            try {
                for (i = 0; i < MAX_SIZE; i++) {
                    vertex.property(String.format("%d", i), RANDOM_STRING);
                }
                Assert.fail("Error, expected RECORD_TOO_BIG exception.");
            } catch (Exception e) {
            }

            vertex = g.V().next();
            final Iterator<VertexProperty<Object>> properties = vertex.properties();
            final Map<String, Object> propertyMap = new HashMap<>();
            while(properties.hasNext()) {
                VertexProperty<Object> property = properties.next();
                propertyMap.put(property.key(), property.value());
            }
            for (int j = 0; j < i; j++) {
                Assert.assertTrue(propertyMap.containsKey(String.format("%d", j)));
                Assert.assertEquals(RANDOM_STRING, propertyMap.get(String.format("%d", j)));
            }
        }
    }

    @Test
    public void testWriteEdgeOverflow() {
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES))) {
            fireflyGraph.getBaseGraph().dropDatabase(fireflyGraph, false);

            final GraphTraversalSource g = fireflyGraph.traversal();
            Vertex vertex = g.addV("vertex_test").next();
            for (int i = 0; i < PROPERTY_COUNT; i++) {
                vertex.property(String.format("%d", i), RANDOM_STRING);
            }

            int i = 0;
            try {
                for (i = 0; i < MAX_SIZE; i++) {
                    vertex.addEdge(RANDOM_STRING, vertex);
                }
                Assert.fail("Error, expected RECORD_TOO_BIG exception.");
            } catch (Exception e) {
            }

            vertex = g.V().next();
            final Iterator<VertexProperty<Object>> properties = vertex.properties();
            final Map<String, Object> propertyMap = new HashMap<>();
            while(properties.hasNext()) {
                VertexProperty<Object> property = properties.next();
                propertyMap.put(property.key(), property.value());
            }
            for (int j = 0; j < PROPERTY_COUNT; j++) {
                Assert.assertTrue(propertyMap.containsKey(String.format("%d", j)));
                Assert.assertEquals(RANDOM_STRING, propertyMap.get(String.format("%d", j)));
            }
            final Iterator<Edge> edges = vertex.edges(Direction.BOTH);
            final List<Edge> edgeList = new ArrayList<>();
            while (edges.hasNext()) {
                edgeList.add(edges.next());
            }
            Assert.assertEquals(i * 2, edgeList.size());
            edgeList.forEach(edge -> Assert.assertEquals(RANDOM_STRING, edge.label()));
        }
    }
}
