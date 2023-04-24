package com.aerospike.firefly.metadata;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class TestFireflyApproximateMetadata extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    private static GraphTraversalSource g;

    @BeforeClass
    public static void getGraphTraversalSource() {
        g = graph.traversal();
    }

    private static class EdgeInfo {
        public String vLabel1;
        public String vLabel2;
        public long count;

        public EdgeInfo(final String vLabel1, final String vLabel2, final long count) {
            this.vLabel1 = vLabel1;
            this.vLabel2 = vLabel2;
            this.count = count;
        }
    }

    @Before
    public void dropSets() {
        // Need to make sure we remove everything.
        g.V().drop().iterate();
    }

    @Test
    public void testEdgeMixedLabelCount() {
        final Map<String, Long> expectedVertexLabelToCount = new HashMap<>();
        expectedVertexLabelToCount.put("movie", 100L);
        expectedVertexLabelToCount.put("actor", 5000L);
        expectedVertexLabelToCount.put("director", 100L);
        expectedVertexLabelToCount.put("writer", 50L);
        expectedVertexLabelToCount.put("genre", 200L);

        final Map<String, Set<String>> expectedVertexLabelToProperties = new HashMap<>();
        expectedVertexLabelToProperties.put("movie", new HashSet<>(List.of("name", "year")));
        expectedVertexLabelToProperties.put("actor", new HashSet<>(List.of("name", "age")));
        expectedVertexLabelToProperties.put("director", new HashSet<>(List.of("name", "age")));
        expectedVertexLabelToProperties.put("writer", new HashSet<>(List.of("name", "age")));
        expectedVertexLabelToProperties.put("genre", new HashSet<>(List.of("name")));

        final Map<String, EdgeInfo> expectedEdgeLabelToVertexLabelPairToCount = new HashMap<>();
        expectedEdgeLabelToVertexLabelPairToCount.put("ACTED_IN", new EdgeInfo("actor", "movie", 5000L));
        expectedEdgeLabelToVertexLabelPairToCount.put("DIRECTED", new EdgeInfo("director", "movie", 100L));
        expectedEdgeLabelToVertexLabelPairToCount.put("WROTE", new EdgeInfo("writer", "movie", 50L));
        expectedEdgeLabelToVertexLabelPairToCount.put("HAS_GENRE", new EdgeInfo("movie", "genre", 20L));

        final Map<String, Set<String>> expectedEdgeLabelToProperties = new HashMap<>();
        expectedEdgeLabelToProperties.put("ACTED_IN", new HashSet<>(List.of("role")));
        expectedEdgeLabelToProperties.put("DIRECTED", new HashSet<>(List.of("credentials")));
        expectedEdgeLabelToProperties.put("WROTE", new HashSet<>(List.of("part")));
        expectedEdgeLabelToProperties.put("HAS_GENRE", new HashSet<>(List.of()));

        int j = 0;
        for (final Map.Entry<String, Long> entry : expectedVertexLabelToCount.entrySet()) {
            final String label = entry.getKey();
            final long expectedCount = entry.getValue();
            for (long i = 0; i < expectedCount; i++) {
                if (++j % 1000 == 0) {
                    System.out.println("Adding vertex " + j);
                }
                final GraphTraversal<?, ?> t = g.addV(label);
                for (String key : expectedVertexLabelToProperties.get(label)) {
                    t.property(key, "FOO");
                }
                t.next();
            }
        }

        final Random random = new Random();
        j = 0;
        final Map<String, List<Vertex>> vertexMappings = new HashMap<>();
        for (final Map.Entry<String, EdgeInfo> entry : expectedEdgeLabelToVertexLabelPairToCount.entrySet()) {
            final String label = entry.getKey();
            final EdgeInfo edgeInfo = entry.getValue();
            if (!vertexMappings.containsKey(edgeInfo.vLabel1)) {
                vertexMappings.put(edgeInfo.vLabel1, g.V().hasLabel(edgeInfo.vLabel1).toList());
            }
            if (!vertexMappings.containsKey(edgeInfo.vLabel2)) {
                vertexMappings.put(edgeInfo.vLabel2, g.V().hasLabel(edgeInfo.vLabel2).toList());
            }
            final long expectedCount = edgeInfo.count;
            for (long i = 0; i < expectedCount; i++) {
                if (++j % 1000 == 0) {
                    System.out.println("Adding edge " + j);
                }

                final GraphTraversal<?, ?> t = g.addE(label);
                for (String key : expectedEdgeLabelToProperties.get(label)) {
                    t.property(key, "FOO");
                }
                t.from(vertexMappings.get(edgeInfo.vLabel1).get(random.nextInt(vertexMappings.get(edgeInfo.vLabel1).size()))).
                        to(vertexMappings.get(edgeInfo.vLabel2).get(random.nextInt(vertexMappings.get(edgeInfo.vLabel2).size()))).iterate();
            }
        }

        // Get statistics vertex.
        wait1Second();
        final Vertex v = g.V("~graph_summary").next();

        // Check vertex properties.
        final Map<String, Set<String>> vertexProperties = v.value("vertex_properties_per_label");
        Assert.assertEquals(expectedVertexLabelToProperties, vertexProperties);

        // Check vertex total count.
        final long vertexCount = v.value("vertex_count");
        final AtomicLong vertexCountSum = new AtomicLong(0);
        expectedVertexLabelToCount.values().forEach(vertexCountSum::addAndGet);
        Assert.assertEquals(vertexCountSum.get(), vertexCount);

        // Check vertex count per label.
        final Map<String, Long> vertexCountPerLabel = v.value("vertex_count_per_label");
        for (final Map.Entry<String, Long> entry : expectedVertexLabelToCount.entrySet()) {
            final String label = entry.getKey();
            Assert.assertTrue(vertexCountPerLabel.containsKey(label));
            final long expectedCount = entry.getValue();
            assertEquals(expectedCount, vertexCountPerLabel.get(label).longValue());
        }


        // Check edge properties.
        final Map<String, Set<String>> edgeProperties = v.value("edge_properties_per_label");
        assertEquals(expectedEdgeLabelToProperties, edgeProperties);

        // Check total edge count.
        final Long edgeCount = v.value("edge_count");
        AtomicLong expectedEdgeCount = new AtomicLong(0);
        expectedEdgeLabelToVertexLabelPairToCount.forEach((k, v1) -> expectedEdgeCount.addAndGet(v1.count));
        assertEquals(expectedEdgeCount.get(), edgeCount.longValue());

        // Check edge count per label.
        final Map<String, Long> edgeCountPerLabel = v.value("edge_count_per_label");
        final Map<String, Long> expectedEdgeCountPerLabel = new HashMap<>();
        expectedEdgeLabelToVertexLabelPairToCount.forEach((k, v1) -> expectedEdgeCountPerLabel.put(k, v1.count));
        assertEquals(expectedEdgeCountPerLabel, edgeCountPerLabel);
    }

    @Test
    public void testVertexMixedLabelCount() {
        final Map<String, Long> expectedVertexLabelToCount = new HashMap<>();
        expectedVertexLabelToCount.put("movie", 100L);
        expectedVertexLabelToCount.put("actor", 5000L);
        expectedVertexLabelToCount.put("director", 100L);
        expectedVertexLabelToCount.put("writer", 50L);
        expectedVertexLabelToCount.put("genre", 200L);

        final Map<String, Set<String>> expectedVertexLabelToProperties = new HashMap<>();
        expectedVertexLabelToProperties.put("movie", new HashSet<>(Arrays.asList("name", "year")));
        expectedVertexLabelToProperties.put("actor", new HashSet<>(Arrays.asList("name", "age")));
        expectedVertexLabelToProperties.put("director", new HashSet<>(Arrays.asList("name", "age")));
        expectedVertexLabelToProperties.put("writer", new HashSet<>(Arrays.asList("name", "age")));
        expectedVertexLabelToProperties.put("genre", new HashSet<>(List.of("name")));

        for (final Map.Entry<String, Long> entry : expectedVertexLabelToCount.entrySet()) {
            final String label = entry.getKey();
            final long expectedCount = entry.getValue();
            for (long i = 0; i < expectedCount; i++) {
                final GraphTraversal<?, ?> t = g.addV(label);
                for (String key : expectedVertexLabelToProperties.get(label)) {
                    t.property(key, "FOO");
                }
                t.next();
            }
        }

        // Get statistics vertex.
        wait1Second();
        final Vertex v = g.V("~graph_summary").next();

        // Check vertex properties per label.
        final Map<String, Set<String>> vertexPropertiesPerLabel = v.value("vertex_properties_per_label");
        Assert.assertEquals(expectedVertexLabelToProperties, vertexPropertiesPerLabel);

        // Check vertex total count.
        final long vertexCount = v.value("vertex_count");
        final AtomicLong vertexCountSum = new AtomicLong(0);
        expectedVertexLabelToCount.values().forEach(vertexCountSum::addAndGet);
        Assert.assertEquals(vertexCountSum.get(), vertexCount);

        // Check vertex count per label.
        final Map<String, Long> vertexCountPerLabel = v.value("vertex_count_per_label");
        for (final Map.Entry<String, Long> entry : expectedVertexLabelToCount.entrySet()) {
            final String label = entry.getKey();
            Assert.assertTrue(vertexCountPerLabel.containsKey(label));
            final long expectedCount = entry.getValue();
            assertEquals(expectedCount, vertexCountPerLabel.get(label).longValue());
        }


        // Validate all edge related fields are empty/zero.
        final Map<String, Set<String>> edgePropertiesPerLabel = v.value("edge_properties_per_label");
        final Map<String, Long> edgeCountPerLabel = v.value("edge_count_per_label");
        final Long edgeCount = v.value("edge_count");
        assertEquals(Map.of(), edgePropertiesPerLabel);
        assertEquals(Map.of(), edgeCountPerLabel);
        assertEquals(0L, edgeCount.longValue());
    }

    @Test
    public void testPoisonPillInsideWriteStream() {
        for (int i = 0; i < 3; i++) {
            final FireflyGraph firefly = FireflyGraph.open(config);
            final GraphTraversalSource g = firefly.traversal();
            for (int j = 0; j < 5000; j++) {
                g.addV("person").property("name", "person" + i).next();
            }
            // Poison pill injected on close, should be mid-stream.
            // Just in case it doesn't end up that way this is looped.
            firefly.close();
            Assert.assertTrue(firefly.fireflySummaryUpdater.exited());
        }
    }

    @Test
    public void testEdgeMixedProperties() {
        final Map<String, Long> expectedVertexLabelToCount = new HashMap<>();
        expectedVertexLabelToCount.put("movie", 100L);
        expectedVertexLabelToCount.put("actor", 5000L);
        expectedVertexLabelToCount.put("director", 100L);
        expectedVertexLabelToCount.put("writer", 50L);
        expectedVertexLabelToCount.put("genre", 200L);

        final Map<String, EdgeInfo> expectedEdgeLabelToVertexLabelPairToCount = new HashMap<>();
        expectedEdgeLabelToVertexLabelPairToCount.put("ACTED_IN", new EdgeInfo("actor", "movie", 5000L));
        expectedEdgeLabelToVertexLabelPairToCount.put("DIRECTED", new EdgeInfo("director", "movie", 100L));
        expectedEdgeLabelToVertexLabelPairToCount.put("WROTE", new EdgeInfo("writer", "movie", 50L));
        expectedEdgeLabelToVertexLabelPairToCount.put("HAS_GENRE", new EdgeInfo("movie", "genre", 20L));

        int j = 0;
        for (final Map.Entry<String, Long> entry : expectedVertexLabelToCount.entrySet()) {
            final String label = entry.getKey();
            final long expectedCount = entry.getValue();
            for (long i = 0; i < expectedCount; i++) {
                if (++j % 1000 == 0) {
                    System.out.println("Adding vertex " + j);
                }
                g.addV(label).next();
            }
        }

        final Random random = new Random();
        j = 0;
        final Map<String, List<Vertex>> vertexMappings = new HashMap<>();
        for (final Map.Entry<String, EdgeInfo> entry : expectedEdgeLabelToVertexLabelPairToCount.entrySet()) {
            final String label = entry.getKey();
            final EdgeInfo edgeInfo = entry.getValue();
            if (!vertexMappings.containsKey(edgeInfo.vLabel1)) {
                vertexMappings.put(edgeInfo.vLabel1, g.V().hasLabel(edgeInfo.vLabel1).toList());
            }
            if (!vertexMappings.containsKey(edgeInfo.vLabel2)) {
                vertexMappings.put(edgeInfo.vLabel2, g.V().hasLabel(edgeInfo.vLabel2).toList());
            }
            final long expectedCount = edgeInfo.count;
            for (long i = 0; i < expectedCount; i++) {
                if (++j % 1000 == 0) {
                    System.out.println("Adding edge " + j);
                }
                g.addE(label).
                        from(vertexMappings.get(edgeInfo.vLabel1).get(random.nextInt(vertexMappings.get(edgeInfo.vLabel1).size()))).
                        to(vertexMappings.get(edgeInfo.vLabel2).get(random.nextInt(vertexMappings.get(edgeInfo.vLabel2).size()))).iterate();
            }
        }

        // Get statistics vertex.
        wait1Second();
        final Vertex v = g.V("~graph_summary").next();

        // Check vertex properties.
        final Map<String, Set<String>> vertexProperties = v.value("vertex_properties_per_label");
        final Map<String, Set<String>> expectedVertexProperties = expectedVertexLabelToCount.keySet().
                stream().map(label -> Map.entry(label, new HashSet<String>())).
                collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Assert.assertEquals(expectedVertexProperties, vertexProperties);

        // Check vertex total count.
        final long vertexCount = v.value("vertex_count");
        final AtomicLong vertexCountSum = new AtomicLong(0);
        expectedVertexLabelToCount.values().forEach(vertexCountSum::addAndGet);
        Assert.assertEquals(vertexCountSum.get(), vertexCount);

        // Check vertex count per label.
        final Map<String, Long> vertexCountPerLabel = v.value("vertex_count_per_label");
        for (final Map.Entry<String, Long> entry : expectedVertexLabelToCount.entrySet()) {
            final String label = entry.getKey();
            Assert.assertTrue(vertexCountPerLabel.containsKey(label));
            final long expectedCount = entry.getValue();
            assertEquals(expectedCount, vertexCountPerLabel.get(label).longValue());
        }


        // Check edge properties.
        final Map<String, Set<String>> edgeProperties = v.value("edge_properties_per_label");
        final Map<String, Set<String>> expectedEdgeProperties = expectedEdgeLabelToVertexLabelPairToCount.keySet().
                stream().map(label -> Map.entry(label, new HashSet<String>())).
                collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertEquals(expectedEdgeProperties, edgeProperties);

        // Check total edge count.
        final Long edgeCount = v.value("edge_count");
        AtomicLong expectedEdgeCount = new AtomicLong(0);
        expectedEdgeLabelToVertexLabelPairToCount.forEach((k, v1) -> expectedEdgeCount.addAndGet(v1.count));
        assertEquals(expectedEdgeCount.get(), edgeCount.longValue());

        // Check edge count per label.
        final Map<String, Long> edgeCountPerLabel = v.value("edge_count_per_label");
        final Map<String, Long> expectedEdgeCountPerLabel = new HashMap<>();
        expectedEdgeLabelToVertexLabelPairToCount.forEach((k, v1) -> expectedEdgeCountPerLabel.put(k, v1.count));
        assertEquals(expectedEdgeCountPerLabel, edgeCountPerLabel);
    }

    @Test
    public void testVertexMixedProperties() {
        final Map<String, Long> expectedVertexLabelToCount = new HashMap<>();
        expectedVertexLabelToCount.put("movie", 100L);
        expectedVertexLabelToCount.put("actor", 5000L);
        expectedVertexLabelToCount.put("director", 100L);
        expectedVertexLabelToCount.put("writer", 50L);
        expectedVertexLabelToCount.put("genre", 200L);

        for (final Map.Entry<String, Long> entry : expectedVertexLabelToCount.entrySet()) {
            final String label = entry.getKey();
            final long expectedCount = entry.getValue();
            for (long i = 0; i < expectedCount; i++) {
                g.addV(label).next();
            }
        }

        // Get statistics vertex.
        wait1Second();
        final Vertex v = g.V("~graph_summary").next();

        // Check vertex properties per label.
        final Map<String, Set<String>> vertexPropertiesPerLabel = v.value("vertex_properties_per_label");
        final Map<String, Set<String>> expectedVertexPropertiesPerLabel = expectedVertexLabelToCount.keySet().
                stream().map(label -> Map.entry(label, new HashSet<String>())).
                collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Assert.assertEquals(expectedVertexPropertiesPerLabel, vertexPropertiesPerLabel);

        // Check vertex total count.
        final long vertexCount = v.value("vertex_count");
        final AtomicLong vertexCountSum = new AtomicLong(0);
        expectedVertexLabelToCount.values().forEach(vertexCountSum::addAndGet);
        Assert.assertEquals(vertexCountSum.get(), vertexCount);

        // Check vertex count per label.
        final Map<String, Long> vertexCountPerLabel = v.value("vertex_count_per_label");
        for (final Map.Entry<String, Long> entry : expectedVertexLabelToCount.entrySet()) {
            final String label = entry.getKey();
            Assert.assertTrue(vertexCountPerLabel.containsKey(label));
            final long expectedCount = entry.getValue();
            assertEquals(expectedCount, vertexCountPerLabel.get(label).longValue());
        }


        // Validate all edge related fields are empty/zero.
        final Map<String, Set<String>> edgePropertiesPerLabel = v.value("edge_properties_per_label");
        final Map<String, Long> edgeCountPerLabel = v.value("edge_count_per_label");
        final Long edgeCount = v.value("edge_count");
        assertEquals(Map.of(), edgePropertiesPerLabel);
        assertEquals(Map.of(), edgeCountPerLabel);
        assertEquals(0L, edgeCount.longValue());
    }

    @Test
    public void testEmpty() {
        // Get statistics vertex.
        final Vertex v = g.V("~graph_summary").next();

        // Validate all vertex related fields are empty/zero.
        final Map<String, Set<String>> vertexPropertiesPerLabel = v.value("vertex_properties_per_label");
        final Map<String, Long> vertexCountPerLabel = v.value("vertex_count_per_label");
        final long vertexCount = v.value("vertex_count");
        assertEquals(Map.of(), vertexPropertiesPerLabel);
        assertEquals(Map.of(), vertexCountPerLabel);
        assertEquals(0L, vertexCount);

        // Validate all edge related fields are empty/zero.
        final Map<String, Set<String>> edgePropertiesPerLabel = v.value("edge_properties_per_label");
        final Map<String, Long> edgeCountPerLabel = v.value("edge_count_per_label");
        final Long edgeCount = v.value("edge_count");
        assertEquals(Map.of(), edgePropertiesPerLabel);
        assertEquals(Map.of(), edgeCountPerLabel);
        assertEquals(0L, edgeCount.longValue());
    }

    @Test
    public void testIncrementalVertexPropertyAdditions() {
        // Get statistics vertex.
        wait1Second();
        Vertex v = g.V("~graph_summary").next();

        // Validate all vertex related fields are empty/zero.
        Map<String, Set<String>> vertexPropertiesPerLabel = v.value("vertex_properties_per_label");
        Map<String, Long> vertexCountPerLabel = v.value("vertex_count_per_label");
        long vertexCount = v.value("vertex_count");
        assertEquals(Map.of(), vertexPropertiesPerLabel);
        assertEquals(Map.of(), vertexCountPerLabel);
        assertEquals(0L, vertexCount);

        // Validate all edge related fields are empty/zero.
        Map<String, Set<String>> edgePropertiesPerLabel = v.value("edge_properties_per_label");
        Map<String, Long> edgeCountPerLabel = v.value("edge_count_per_label");
        Long edgeCount = v.value("edge_count");
        assertEquals(Map.of(), edgePropertiesPerLabel);
        assertEquals(Map.of(), edgeCountPerLabel);
        assertEquals(0L, edgeCount.longValue());

        g.addV("person").iterate();

        // Get statistics vertex.
        wait1Second();
        v = g.V("~graph_summary").next();

        // Validate all vertex related fields are empty/zero.
        vertexPropertiesPerLabel = v.value("vertex_properties_per_label");
        vertexCountPerLabel = v.value("vertex_count_per_label");
        vertexCount = v.value("vertex_count");
        assertEquals(Map.of("person", new HashSet<String>()), vertexPropertiesPerLabel);
        assertEquals(Map.of("person", 1L), vertexCountPerLabel);
        assertEquals(1L, vertexCount);

        // Validate all edge related fields are empty/zero.
        edgePropertiesPerLabel = v.value("edge_properties_per_label");
        edgeCountPerLabel = v.value("edge_count_per_label");
        edgeCount = v.value("edge_count");
        assertEquals(Map.of(), edgePropertiesPerLabel);
        assertEquals(Map.of(), edgeCountPerLabel);
        assertEquals(0L, edgeCount.longValue());

        g.addV("dog").iterate();
        g.addV("person").property("name", "Lyndon").iterate();
        g.V().hasLabel("person").property("age", 29L).iterate();

        // Get statistics vertex.
        wait1Second();
        v = g.V("~graph_summary").next();

        // Validate all vertex related fields are empty/zero.
        vertexPropertiesPerLabel = v.value("vertex_properties_per_label");
        vertexCountPerLabel = v.value("vertex_count_per_label");
        vertexCount = v.value("vertex_count");
        assertEquals(Map.of("person", Set.of("name", "age"), "dog", Set.of()), vertexPropertiesPerLabel);
        assertEquals(Map.of("person", 2L, "dog", 1L), vertexCountPerLabel);
        assertEquals(3L, vertexCount);

        // Validate all edge related fields are empty/zero.
        edgePropertiesPerLabel = v.value("edge_properties_per_label");
        edgeCountPerLabel = v.value("edge_count_per_label");
        edgeCount = v.value("edge_count");
        assertEquals(Map.of(), edgePropertiesPerLabel);
        assertEquals(Map.of(), edgeCountPerLabel);
        assertEquals(0L, edgeCount.longValue());

        g.addE("OWNS").
                from(
                        __.V().hasLabel("person").has("name", "Lyndon")).
                to(
                        __.V().hasLabel("dog")).iterate();
        g.E().hasLabel("OWNS").property("since", 2019L).iterate();
        g.addE("OWNS").property("foo", "bar").
                from(
                        __.V().hasLabel("person").has("name", "Lyndon")).
                to(
                        __.V().hasLabel("dog")).iterate();
        g.addE("OWNED_BY").property("baz", 1L).
                from(
                        __.V().hasLabel("dog")).
                to(
                        __.V().hasLabel("person").has("name", "Lyndon")).iterate();

        // Get statistics vertex.
        wait1Second();
        v = g.V("~graph_summary").next();

        // Validate all vertex related fields are empty/zero.
        vertexPropertiesPerLabel = v.value("vertex_properties_per_label");
        vertexCountPerLabel = v.value("vertex_count_per_label");
        vertexCount = v.value("vertex_count");
        assertEquals(Map.of("person", Set.of("name", "age"), "dog", Set.of()), vertexPropertiesPerLabel);
        assertEquals(Map.of("person", 2L, "dog", 1L), vertexCountPerLabel);
        assertEquals(3L, vertexCount);

        // Validate all edge related fields are empty/zero.
        edgePropertiesPerLabel = v.value("edge_properties_per_label");
        edgeCountPerLabel = v.value("edge_count_per_label");
        edgeCount = v.value("edge_count");
        assertEquals(Map.of("OWNS", Set.of("since", "foo"), "OWNED_BY", Set.of("baz")), edgePropertiesPerLabel);
        assertEquals(Map.of("OWNS", 2L, "OWNED_BY", 1L), edgeCountPerLabel);
        assertEquals(3L, edgeCount.longValue());
    }

    @Test
    public void testLongVertexLabel() {
        g.addV("123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890").iterate();
        wait1Second();
        Assert.assertFalse(graph.fireflySummaryUpdater.exited());
    }

    @Test
    public void testLongEdgeLabel() {
        final Vertex a = g.addV("person").next();
        final Vertex b = g.addV("person").next();

        g.addE("123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890").from(a).to(b).iterate();
        wait1Second();
        Assert.assertFalse(graph.fireflySummaryUpdater.exited());
    }

    @Test
    public void testLongVertexProperty() {
        g.addV("1").property("123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890", "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890").iterate();
        wait1Second();
        Assert.assertFalse(graph.fireflySummaryUpdater.exited());
    }

    @Test
    public void testLongEdgeProperty() {
        final Vertex a = g.addV("person").next();
        final Vertex b = g.addV("person").next();

        g.addE("1").property("123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890", "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890").from(a).to(b).iterate();
        wait1Second();
        Assert.assertFalse(graph.fireflySummaryUpdater.exited());
    }

    void wait1Second() {
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
