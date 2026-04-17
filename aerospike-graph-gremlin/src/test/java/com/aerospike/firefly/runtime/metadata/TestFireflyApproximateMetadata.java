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

package com.aerospike.firefly.runtime.metadata;

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

        // Get statistics.
        wait5Seconds();
        final Map<Object, Object> summary = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();

        // Check vertex properties.
        final Map<String, Set<String>> vertexProperties = (Map<String, Set<String>>)summary.get("Vertex properties by label");
        Assert.assertEquals(expectedVertexLabelToProperties, vertexProperties);

        // Check vertex total count.
        final long vertexCount = (long)summary.get("Total vertex count");
        final AtomicLong vertexCountSum = new AtomicLong(0);
        expectedVertexLabelToCount.values().forEach(vertexCountSum::addAndGet);
        Assert.assertEquals(vertexCountSum.get(), vertexCount);

        // Check vertex count per label.
        final Map<String, Long> vertexCountPerLabel = (Map<String, Long>)summary.get("Vertex count by label");
        for (final Map.Entry<String, Long> entry : expectedVertexLabelToCount.entrySet()) {
            final String label = entry.getKey();
            Assert.assertTrue(vertexCountPerLabel.containsKey(label));
            final long expectedCount = entry.getValue();
            assertEquals(expectedCount, vertexCountPerLabel.get(label).longValue());
        }


        // Check edge properties.
        final Map<String, Set<String>> edgeProperties = (Map<String, Set<String>>)summary.get("Edge properties by label");
        assertEquals(expectedEdgeLabelToProperties, edgeProperties);

        // Check total edge count.
        final Long edgeCount = (Long)summary.get("Total edge count");
        AtomicLong expectedEdgeCount = new AtomicLong(0);
        expectedEdgeLabelToVertexLabelPairToCount.forEach((k, v1) -> expectedEdgeCount.addAndGet(v1.count));
        assertEquals(expectedEdgeCount.get(), edgeCount.longValue());

        // Check edge count per label.
        final Map<String, Long> edgeCountPerLabel = (Map<String, Long>)summary.get("Edge count by label");
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

        // Get statistics.
        wait5Seconds();
        final Map<Object, Object> summary = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();

        // Check vertex properties per label.
        final Map<String, Set<String>> vertexPropertiesPerLabel = (Map<String, Set<String>> )summary.get("Vertex properties by label");
        Assert.assertEquals(expectedVertexLabelToProperties, vertexPropertiesPerLabel);

        // Check vertex total count.
        final Long vertexCount = (Long)summary.get("Total vertex count");
        final AtomicLong vertexCountSum = new AtomicLong(0);
        expectedVertexLabelToCount.values().forEach(vertexCountSum::addAndGet);
        Assert.assertEquals(vertexCountSum.get(), vertexCount.longValue());

        // Check vertex count per label.
        final Map<String, Long> vertexCountPerLabel = (Map<String, Long>)summary.get("Vertex count by label");
        for (final Map.Entry<String, Long> entry : expectedVertexLabelToCount.entrySet()) {
            final String label = entry.getKey();
            Assert.assertTrue(vertexCountPerLabel.containsKey(label));
            final long expectedCount = entry.getValue();
            assertEquals(expectedCount, vertexCountPerLabel.get(label).longValue());
        }


        // Validate all edge related fields are empty/zero.
        final Map<String, Set<String>> edgePropertiesPerLabel = (Map<String, Set<String>>) summary.get("Edge properties by label");
        final Map<String, Long> edgeCountPerLabel = (Map<String, Long>) summary.get("Edge count by label");
        final Long edgeCount = (Long) summary.get("Total edge count");
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
        }
        Assert.assertFalse(graph.fireflySummaryUpdater.exited());
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

        // Get statistics.
        wait5Seconds();
        final Map<Object, Object> summary = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();

        // Check vertex properties.
        final Map<String, Set<String>> vertexProperties = (Map<String, Set<String>>) summary.get("Vertex properties by label");
        final Map<String, Set<String>> expectedVertexProperties = expectedVertexLabelToCount.keySet().
                stream().map(label -> Map.entry(label, new HashSet<String>())).
                collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Assert.assertEquals(expectedVertexProperties, vertexProperties);

        // Check vertex total count.
        final long vertexCount = (long) summary.get("Total vertex count");
        final AtomicLong vertexCountSum = new AtomicLong(0);
        expectedVertexLabelToCount.values().forEach(vertexCountSum::addAndGet);
        Assert.assertEquals(vertexCountSum.get(), vertexCount);

        // Check vertex count per label.
        final Map<String, Long> vertexCountPerLabel = (Map<String, Long>) summary.get("Vertex count by label");
        for (final Map.Entry<String, Long> entry : expectedVertexLabelToCount.entrySet()) {
            final String label = entry.getKey();
            Assert.assertTrue(vertexCountPerLabel.containsKey(label));
            final long expectedCount = entry.getValue();
            assertEquals(expectedCount, vertexCountPerLabel.get(label).longValue());
        }


        // Check edge properties.
        final Map<String, Set<String>> edgeProperties = (Map<String, Set<String>>) summary.get("Edge properties by label");
        final Map<String, Set<String>> expectedEdgeProperties = expectedEdgeLabelToVertexLabelPairToCount.keySet().
                stream().map(label -> Map.entry(label, new HashSet<String>())).
                collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        assertEquals(expectedEdgeProperties, edgeProperties);

        // Check total edge count.
        final Long edgeCount = (Long) summary.get("Total edge count");
        AtomicLong expectedEdgeCount = new AtomicLong(0);
        expectedEdgeLabelToVertexLabelPairToCount.forEach((k, v1) -> expectedEdgeCount.addAndGet(v1.count));
        assertEquals(expectedEdgeCount.get(), edgeCount.longValue());

        // Check edge count per label.
        final Map<String, Long> edgeCountPerLabel = (Map<String, Long>) summary.get("Edge count by label");
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

        // Get statistics.
        wait5Seconds();
        final Map<Object, Object> summary = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();

        // Check vertex properties per label.
        final Map<String, Set<String>> vertexPropertiesPerLabel = (Map<String, Set<String>>) summary.get("Vertex properties by label");
        final Map<String, Set<String>> expectedVertexPropertiesPerLabel = expectedVertexLabelToCount.keySet().
                stream().map(label -> Map.entry(label, new HashSet<String>())).
                collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
        Assert.assertEquals(expectedVertexPropertiesPerLabel, vertexPropertiesPerLabel);

        // Check vertex total count.
        final Long vertexCount = (Long) summary.get("Total vertex count");
        final AtomicLong vertexCountSum = new AtomicLong(0);
        expectedVertexLabelToCount.values().forEach(vertexCountSum::addAndGet);
        Assert.assertEquals(vertexCountSum.get(), vertexCount.longValue());

        // Check vertex count per label.
        final Map<String, Long> vertexCountPerLabel = (Map<String, Long>) summary.get("Vertex count by label");
        for (final Map.Entry<String, Long> entry : expectedVertexLabelToCount.entrySet()) {
            final String label = entry.getKey();
            Assert.assertTrue(vertexCountPerLabel.containsKey(label));
            final long expectedCount = entry.getValue();
            assertEquals(expectedCount, vertexCountPerLabel.get(label).longValue());
        }

        // Validate all edge related fields are empty/zero.
        final Map<String, Set<String>> edgePropertiesPerLabel = (Map<String, Set<String>>) summary.get("Edge properties by label");
        final Map<String, Long> edgeCountPerLabel = (Map<String, Long>) summary.get("Edge count by label");
        final Long edgeCount = (Long) summary.get("Total edge count");
        assertEquals(Map.of(), edgePropertiesPerLabel);
        assertEquals(Map.of(), edgeCountPerLabel);
        assertEquals(0L, edgeCount.longValue());
    }

    @Test
    public void testEmpty() {
        final Map<Object, Object> summary = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();

        // Validate all vertex related fields are empty/zero.
        final Map<String, Set<String>> vertexPropertiesPerLabel = (Map<String, Set<String>>)summary.get("Vertex properties by label");
        final Map<String, Long> vertexCountPerLabel = (Map<String, Long>)summary.get("Vertex count by label");
        final Long vertexCount = (Long)summary.get("Total vertex count");
        assertEquals(Map.of(), vertexPropertiesPerLabel);
        assertEquals(Map.of(), vertexCountPerLabel);
        assertEquals(0L, vertexCount.longValue());

        // Validate all edge related fields are empty/zero.
        final Map<String, Set<String>> edgePropertiesPerLabel = (Map<String, Set<String>>)summary.get("Edge properties by label");
        final Map<String, Long> edgeCountPerLabel = (Map<String, Long>)summary.get("Edge count by label");
        final Long edgeCount = (Long)summary.get("Total edge count");
        assertEquals(Map.of(), edgePropertiesPerLabel);
        assertEquals(Map.of(), edgeCountPerLabel);
        assertEquals(0L, edgeCount.longValue());
    }

    @Test
    public void testIncrementalVertexPropertyAdditions() {
        // Get statistics.
        wait5Seconds();
        Map<Object, Object> summary = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();

        // Validate all vertex related fields are empty/zero.
        Map<String, Set<String>> vertexPropertiesPerLabel = (Map<String, Set<String>>) summary.get("Vertex properties by label");
        Map<String, Long> vertexCountPerLabel = (Map<String, Long>) summary.get("Vertex count by label");
        Long vertexCount = (Long) summary.get("Total vertex count");
        assertEquals(Map.of(), vertexPropertiesPerLabel);
        assertEquals(Map.of(), vertexCountPerLabel);
        assertEquals(0L, vertexCount.longValue());

        // Validate all edge related fields are empty/zero.
        Map<String, Set<String>> edgePropertiesPerLabel = (Map<String, Set<String>>) summary.get("Edge properties by label");
        Map<String, Long> edgeCountPerLabel = (Map<String, Long>) summary.get("Edge count by label");
        Long edgeCount = (Long) summary.get("Total edge count");
        assertEquals(Map.of(), edgePropertiesPerLabel);
        assertEquals(Map.of(), edgeCountPerLabel);
        assertEquals(0L, edgeCount.longValue());

        g.addV("person").iterate();

        // Get statistics.
        wait5Seconds();
        summary = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();

        // Validate all vertex related fields are empty/zero.
        vertexPropertiesPerLabel = (Map<String, Set<String>>) summary.get("Vertex properties by label");
        vertexCountPerLabel = (Map<String, Long>) summary.get("Vertex count by label");
        vertexCount = (Long) summary.get("Total vertex count");
        assertEquals(Map.of("person", new HashSet<String>()), vertexPropertiesPerLabel);
        assertEquals(Map.of("person", 1L), vertexCountPerLabel);
        assertEquals(1L, vertexCount.longValue());

        // Validate all edge related fields are empty/zero.
        edgePropertiesPerLabel = (Map<String, Set<String>>) summary.get("Edge properties by label");
        edgeCountPerLabel = (Map<String, Long>) summary.get("Edge count by label");
        edgeCount = (Long) summary.get("Total edge count");
        assertEquals(Map.of(), edgePropertiesPerLabel);
        assertEquals(Map.of(), edgeCountPerLabel);
        assertEquals(0L, edgeCount.longValue());

        g.addV("dog").iterate();
        g.addV("person").property("name", "Alice").iterate();
        g.V().hasLabel("person").property("age", 29L).iterate();

        // Get statistics vertex.
        wait5Seconds();
        summary = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();

        // Validate all vertex related fields are empty/zero.
        vertexPropertiesPerLabel = (Map<String, Set<String>>) summary.get("Vertex properties by label");
        vertexCountPerLabel = (Map<String, Long>) summary.get("Vertex count by label");
        vertexCount = (Long) summary.get("Total vertex count");
        assertEquals(Map.of("person", Set.of("name", "age"), "dog", Set.of()), vertexPropertiesPerLabel);
        assertEquals(Map.of("person", 2L, "dog", 1L), vertexCountPerLabel);
        assertEquals(3L, vertexCount.longValue());

        // Validate all edge related fields are empty/zero.
        edgePropertiesPerLabel = (Map<String, Set<String>>) summary.get("Edge properties by label");
        edgeCountPerLabel = (Map<String, Long>) summary.get("Edge count by label");
        edgeCount = (Long) summary.get("Total edge count");
        assertEquals(Map.of(), edgePropertiesPerLabel);
        assertEquals(Map.of(), edgeCountPerLabel);
        assertEquals(0L, edgeCount.longValue());

        g.addE("OWNS").
                from(
                        __.V().hasLabel("person").has("name", "Alice")).
                to(
                        __.V().hasLabel("dog")).iterate();
        g.E().hasLabel("OWNS").property("since", 2019L).iterate();
        g.addE("OWNS").property("foo", "bar").
                from(
                        __.V().hasLabel("person").has("name", "Alice")).
                to(
                        __.V().hasLabel("dog")).iterate();
        g.addE("OWNED_BY").property("baz", 1L).
                from(
                        __.V().hasLabel("dog")).
                to(
                        __.V().hasLabel("person").has("name", "Alice")).iterate();

        // Get statistics vertex.
        wait5Seconds();
        summary = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();

        // Validate all vertex related fields are empty/zero.
        vertexPropertiesPerLabel = (Map<String, Set<String>>) summary.get("Vertex properties by label");
        vertexCountPerLabel = (Map<String, Long>) summary.get("Vertex count by label");
        vertexCount = (Long) summary.get("Total vertex count");
        assertEquals(Map.of("person", Set.of("name", "age"), "dog", Set.of()), vertexPropertiesPerLabel);
        assertEquals(Map.of("person", 2L, "dog", 1L), vertexCountPerLabel);
        assertEquals(3L, vertexCount.longValue());

        // Validate all edge related fields are empty/zero.
        edgePropertiesPerLabel = (Map<String, Set<String>>) summary.get("Edge properties by label");
        edgeCountPerLabel = (Map<String, Long>) summary.get("Edge count by label");
        edgeCount = (Long) summary.get("Total edge count");
        assertEquals(Map.of("OWNS", Set.of("since", "foo"), "OWNED_BY", Set.of("baz")), edgePropertiesPerLabel);
        assertEquals(Map.of("OWNS", 2L, "OWNED_BY", 1L), edgeCountPerLabel);
        assertEquals(3L, edgeCount.longValue());
    }

    @Test
    public void testAddRemoveEdge() {
        final Map<String, Long> expectedVertexLabelToCount = new HashMap<>();
        expectedVertexLabelToCount.put("movie", 100L);
        expectedVertexLabelToCount.put("actor", 5000L);
        expectedVertexLabelToCount.put("director", 100L);
        expectedVertexLabelToCount.put("writer", 50L);
        expectedVertexLabelToCount.put("genre", 200L);

        final Map<String, EdgeInfo> expectedEdgeLabelToVertexLabelPairToCount = new HashMap<>();
        expectedEdgeLabelToVertexLabelPairToCount.put("ACTED_IN", new EdgeInfo("actor", "movie", 2500L));
        expectedEdgeLabelToVertexLabelPairToCount.put("DIRECTED", new EdgeInfo("director", "movie", 50L));
        expectedEdgeLabelToVertexLabelPairToCount.put("WROTE", new EdgeInfo("writer", "movie", 25L));
        expectedEdgeLabelToVertexLabelPairToCount.put("HAS_GENRE", new EdgeInfo("movie", "genre", 10L));


        final Map<String, Set<String>> expectedEdgeLabelToProperties = new HashMap<>();
        expectedEdgeLabelToProperties.put("ACTED_IN", new HashSet<>(List.of("role")));
        expectedEdgeLabelToProperties.put("DIRECTED", new HashSet<>(List.of("credentials")));
        expectedEdgeLabelToProperties.put("WROTE", new HashSet<>(List.of("part")));
        expectedEdgeLabelToProperties.put("HAS_GENRE", new HashSet<>(List.of()));

        final Map<String, Long> expectedEdgeRemoveLabelCount = new HashMap<>();
        expectedEdgeRemoveLabelCount.put("ACTED_IN", 250L);
        expectedEdgeRemoveLabelCount.put("DIRECTED", 25L);
        expectedEdgeRemoveLabelCount.put("WROTE", 12L);
        expectedEdgeRemoveLabelCount.put("HAS_GENRE", 5L);

        int j = 0;
        for (final Map.Entry<String, Long> entry : expectedVertexLabelToCount.entrySet()) {
            final String label = entry.getKey();
            final long expectedCount = entry.getValue();
            for (long i = 0; i < expectedCount; i++) {
                if (++j % 1000 == 0) {
                    System.out.println("Adding vertex " + j);
                }
                final GraphTraversal<?, ?> t = g.addV(label);
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
                t.from(vertexMappings.get(edgeInfo.vLabel1).get(random.nextInt(vertexMappings.get(edgeInfo.vLabel1).size()))).
                        to(vertexMappings.get(edgeInfo.vLabel2).get(random.nextInt(vertexMappings.get(edgeInfo.vLabel2).size()))).iterate();
            }
        }

        for (final Map.Entry<String, Long> entry : expectedEdgeRemoveLabelCount.entrySet()) {
            final String label = entry.getKey();
            final long expectedCount = entry.getValue();
            g.E().hasLabel(label).limit(expectedCount).drop().iterate();
        }

        // Get statistics vertex.
        wait5Seconds();

        final Map<String, Long> edgeCountPerLabel = graph.fireflySummaryUpdater.getFireflyStatistics().edgeCountByLabel();

        for (final Map.Entry<String, Long> entry : edgeCountPerLabel.entrySet()) {
            Assert.assertEquals(
                    (Long) (expectedEdgeLabelToVertexLabelPairToCount.get(entry.getKey()).count - expectedEdgeRemoveLabelCount.get(entry.getKey())),
                    entry.getValue());
        }
    }

    @Test
    public void testAddRemoveVertex() {
        final Map<String, Long> expectedVertexLabelToCount = new HashMap<>();
        expectedVertexLabelToCount.put("movie", 100L);
        expectedVertexLabelToCount.put("actor", 5000L);
        expectedVertexLabelToCount.put("director", 100L);
        expectedVertexLabelToCount.put("writer", 50L);
        expectedVertexLabelToCount.put("genre", 200L);

        final Map<String, Long> expectedVertexRemoveLabelToCount = new HashMap<>();
        expectedVertexRemoveLabelToCount.put("movie", 50L);
        expectedVertexRemoveLabelToCount.put("actor", 2500L);
        expectedVertexRemoveLabelToCount.put("director", 50L);
        expectedVertexRemoveLabelToCount.put("writer", 25L);
        expectedVertexRemoveLabelToCount.put("genre", 100L);

        final Map<String, EdgeInfo> expectedEdgeLabelToVertexLabelPairToCount = new HashMap<>();
        expectedEdgeLabelToVertexLabelPairToCount.put("ACTED_IN", new EdgeInfo("actor", "movie", 2500L));
        expectedEdgeLabelToVertexLabelPairToCount.put("DIRECTED", new EdgeInfo("director", "movie", 50L));
        expectedEdgeLabelToVertexLabelPairToCount.put("WROTE", new EdgeInfo("writer", "movie", 25L));
        expectedEdgeLabelToVertexLabelPairToCount.put("HAS_GENRE", new EdgeInfo("movie", "genre", 10L));


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
                t.from(vertexMappings.get(edgeInfo.vLabel1).get(random.nextInt(vertexMappings.get(edgeInfo.vLabel1).size()))).
                        to(vertexMappings.get(edgeInfo.vLabel2).get(random.nextInt(vertexMappings.get(edgeInfo.vLabel2).size()))).iterate();
            }
        }

        for (final Map.Entry<String, Long> entry : expectedVertexRemoveLabelToCount.entrySet()) {
            final String label = entry.getKey();
            final long expectedCount = entry.getValue();
            g.V().hasLabel(label).limit(expectedCount).drop().iterate();
        }

        // Get statistics vertex.
        wait5Seconds();

        final Map<String, Long> vertexCountPerLabel = graph.fireflySummaryUpdater.getFireflyStatistics().vertexCountByLabel();

        for (final Map.Entry<String, Long> entry : vertexCountPerLabel.entrySet()) {
            Assert.assertEquals(
                    (Long) (expectedVertexLabelToCount.get(entry.getKey()) - expectedVertexRemoveLabelToCount.get(entry.getKey())),
                    entry.getValue());
        }
    }

    @Test
    public void testAddRemoveSimple() {
        final Vertex v = g.addV("1").next();
        g.addE("2").from(v).to(v).iterate();
        wait5Seconds();
        final Map<String, Long> vertexCountPerLabel = graph.fireflySummaryUpdater.getFireflyStatistics().vertexCountByLabel();
        Assert.assertEquals(1L, (long) vertexCountPerLabel.get("1"));
        final Map<String, Long> edgeCountByLabel = graph.fireflySummaryUpdater.getFireflyStatistics().edgeCountByLabel();
        Assert.assertEquals(1L, (long) edgeCountByLabel.get("2"));

        g.V(v.id()).drop().iterate();
        wait5Seconds();
        final Map<String, Long> vertexCountPerLabel2 = graph.fireflySummaryUpdater.getFireflyStatistics().vertexCountByLabel();
        Assert.assertEquals(0L, (long) vertexCountPerLabel2.get("1"));
        final Map<String, Long> edgeCountByLabel2 = graph.fireflySummaryUpdater.getFireflyStatistics().edgeCountByLabel();
        Assert.assertEquals(0L, (long) edgeCountByLabel2.get("2"));
    }

    @Test
    public void testLongVertexLabel() {
        g.addV("123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890").iterate();
        wait5Seconds();
        Assert.assertFalse(graph.fireflySummaryUpdater.exited());
    }

    @Test
    public void testLongEdgeLabel() {
        final Vertex a = g.addV("person").next();
        final Vertex b = g.addV("person").next();

        g.addE("123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890").from(a).to(b).iterate();
        wait5Seconds();
        Assert.assertFalse(graph.fireflySummaryUpdater.exited());
    }

    @Test
    public void testLongVertexProperty() {
        g.addV("1").property("123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890", "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890").iterate();
        wait5Seconds();
        Assert.assertFalse(graph.fireflySummaryUpdater.exited());
    }

    @Test
    public void testLongEdgeProperty() {
        final Vertex a = g.addV("person").next();
        final Vertex b = g.addV("person").next();

        g.addE("1").property("123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890", "123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890").from(a).to(b).iterate();
        wait5Seconds();
        Assert.assertFalse(graph.fireflySummaryUpdater.exited());
    }

    void wait5Seconds() {
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
