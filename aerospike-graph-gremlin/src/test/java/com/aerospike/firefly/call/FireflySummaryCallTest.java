package com.aerospike.firefly.call;

import com.aerospike.firefly.runtime.tasks.FireflyGraphSummaryUpdater;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.process.call.metadata.MetadataServiceSummary.PRETTY_PRINT_FORMAT_SYSTEM;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflySummaryCallTest extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void testMetadataVersion() {
        final GraphTraversalSource g = graph.traversal();
        final Map<String, String> version = (Map<String, String>) g.call("aerospike.graph.admin.metadata.version").next();
        Assert.assertNotNull(version);
        Assert.assertEquals(version.get("Aerospike Graph Service version"), FireflyGraph.FIREFLY_VERSION);
        Assert.assertTrue(version.containsKey("Aerospike version"));
        Assert.assertTrue(version.containsKey("Gremlin version"));
    }

    @Test
    public void testMetadataConfig() {
        final Iterator<String> keys = graph.configuration().getKeys();
        final Map<String, Object> configurationMap = new HashMap<>();
        while (keys.hasNext()) {
            final String key = keys.next();
            if (!key.contains("password") && !key.contains("secret") && !key.contains("token")) {
                configurationMap.put(key, graph.configuration().getProperty(key));
            } else {
                configurationMap.put(key, "********");
            }
        }
        // Unified and gremlin server configs are not available unless running in docker.
        final Map<String, Object> expectedConfig = new HashMap<>();
        expectedConfig.put("Graph Properties", configurationMap);
        expectedConfig.put("Gremlin Server Configuration", "Not available");

        final GraphTraversalSource g = graph.traversal();
        final Map<String, String> config = (Map<String, String>) g.call("aerospike.graph.admin.metadata.config").next();
        Assert.assertEquals(expectedConfig, config);
    }

    @Test
    public void testSummary() throws InterruptedException {
        final GraphTraversalSource g = graph.traversal();
        g.V().drop().iterate();
        Thread.sleep(100);
        final Map<Object, Object> summaryCallEmpty = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();
        final Map<Object, Object> expectedEmpty = Map.of(
                "Vertex count by label", Map.of(),
                "Edge count by label", Map.of(),
                "Supernode count by label", Map.of(),
                "Edge properties by label", Map.of(),
                "Vertex properties by label", Map.of(),
                "Total vertex count", 0L,
                "Total edge count", 0L,
                "Total supernode count", 0L);
        Assert.assertEquals(expectedEmpty, summaryCallEmpty);
        GraphHelper.cloneElements(TinkerFactory.createGratefulDead(), graph);
        waitForSummaryUpdate(graph);
        final long vertexCount = g.V().count().next();
        final long edgeCount = g.E().count().next();
        final long supernodeCount = g.V()
                .filter(__.bothE().count().is(P.gt(graph.getBaseGraph().getConfig().onRecordIdLimit)))
                .count()
                .next();
        final Map<Object, Object> vertexLabels = g.V().group().by(__.label()).by(__.count()).next();
        final Map<Object, Object> edgeLabels = g.E().group().by(__.label()).by(__.count()).next();
        final Map<Object, Object> supernodeLabels = g.V()
                .filter(__.outE().count().is(P.gt(graph.getBaseGraph().getConfig().onRecordIdLimit)))
                .group()
                .by(__.label())
                .by(__.count()).next();

        final Map<Object, Object> vertexProperties = g.V().group().by(__.label()).by(__.properties().key().dedup().fold()).next();
        final Map<Object, Object> edgeProperties = g.E().group().by(__.label()).by(__.properties().key().dedup().fold()).next();
        for (final Object key : edgeLabels.keySet()) {
            if (!edgeProperties.containsKey(key)) {
                edgeProperties.put(key, List.of());
            }
        }
        for (final Object key : vertexProperties.keySet()) {
            if (!vertexProperties.containsKey(key)) {
                vertexProperties.put(key, List.of());
            }
        }
        vertexProperties.replaceAll((k, v) -> new HashSet<>((List<Object>) vertexProperties.get(k)));
        edgeProperties.replaceAll((k, v) -> new HashSet<>((List<Object>) edgeProperties.get(k)));

        final Map<Object, Object> expectedGrateful = Map.of(
                "Vertex count by label", vertexLabels,
                "Edge count by label", edgeLabels,
                "Supernode count by label", supernodeLabels,
                "Edge properties by label", edgeProperties,
                "Vertex properties by label", vertexProperties,
                "Total vertex count", vertexCount,
                "Total edge count", edgeCount,
                "Total supernode count", supernodeCount);
        final Map<Object, Object> summaryCallGrateful = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();
        Assert.assertEquals(expectedGrateful, summaryCallGrateful);
    }

    @Test
    public void testSummaryBulkLoaderStaging() throws InterruptedException {
        FireflyGraphSummaryUpdater summary = graph.fireflySummaryUpdater;
        summary.startVertexPartition(1);
        summary.startVertexPartition(2);
        summary.stageVertexWriteToQueue("lyndon", Set.of("foo1"), 1);
        summary.stageVertexWriteToQueue("lyndon", Set.of("foo2"), 1);
        summary.stageVertexWriteToQueue("lyndon", Set.of("foo3"), 1);
        summary.stageVertexWriteToQueue("lyndon", Set.of("foo1", "foo2"), 2);
        summary.stageVertexWriteToQueue("lyndon", Set.of(), 2);
        summary.stageVertexWriteToQueue("lyndon", Set.of(), 2);
        summary.startEdgePartition(1);
        summary.startEdgePartition(2);
        summary.stageEdgeWriteToQueue("lyndon1", Set.of("foo1"), 1);
        summary.stageEdgeWriteToQueue("lyndon1", Set.of("foo2"), 1);
        summary.stageEdgeWriteToQueue("lyndon1", Set.of("foo1", "foo2"), 1);
        summary.stageEdgeWriteToQueue("lyndon1", Set.of("foo1", "foo2", "foo3"), 2);
        summary.stageEdgeWriteToQueue("lyndon1", Set.of("foo4"), 2);
        summary.startSupernodePartition(1);
        summary.startSupernodePartition(2);
        summary.stageSupernodeWriteToQueue("lyndon", 1);
        summary.stageSupernodeWriteToQueue("lyndon", 1);
        summary.stageSupernodeWriteToQueue("lyndon", 1);
        summary.stageSupernodeWriteToQueue("lyndon", 2);
        summary.stageSupernodeWriteToQueue("lyndon", 2);
        summary.stageSupernodeWriteToQueue("lyndon", 2);
        waitForSummaryUpdate(graph);

        // Verify bulk load sees staged data
        FireflyGraphSummaryUpdater.FireflyElementMetadata elementData = summary.getFireflyStatistics(true);
        Assert.assertTrue(elementData.vertexInfo.containsKey("lyndon"));
        Assert.assertEquals(6L, elementData.vertexInfo.get("lyndon").count);
        Assert.assertTrue(elementData.vertexInfo.get("lyndon").properties.containsAll(Set.of("foo1", "foo2", "foo3")));
        Assert.assertTrue(elementData.edgeInfo.containsKey("lyndon1"));
        Assert.assertEquals(5L, elementData.edgeInfo.get("lyndon1").count);
        Assert.assertTrue(elementData.edgeInfo.get("lyndon1").properties.containsAll(Set.of("foo1", "foo2", "foo3", "foo4")));
        Assert.assertTrue(elementData.supernodeInfo.containsKey("lyndon"));
        Assert.assertEquals(6L, elementData.supernodeInfo.get("lyndon").count);

        // Verify non-bulk load data doesn't see the counts (keys are persisted).
        elementData = summary.getFireflyStatistics(false);
        Assert.assertTrue(elementData.vertexInfo.containsKey("lyndon"));
        Assert.assertTrue(elementData.edgeInfo.containsKey("lyndon1"));
        // Key is not persisted for supernodes since we don't keep properties for supernodes, in addition there won't be
        // a count (not even 0).
        Assert.assertFalse(elementData.supernodeInfo.containsKey("lyndon"));
        Assert.assertEquals(0L, elementData.vertexInfo.get("lyndon").count);
        Assert.assertEquals(0L, elementData.edgeInfo.get("lyndon1").count);
        Assert.assertTrue(elementData.vertexInfo.get("lyndon").properties.containsAll(Set.of("foo1", "foo2", "foo3")));
        Assert.assertTrue(elementData.edgeInfo.get("lyndon1").properties.containsAll(Set.of("foo1", "foo2", "foo3", "foo4")));
        summary.completeVertexPartition(1);
        summary.completeVertexPartition(2);
        summary.completeEdgePartition(1);
        summary.completeEdgePartition(2);
        summary.completeSupernodePartition(1);
        summary.completeSupernodePartition(2);

        // Check that results were moved to non-bulk load data.
        waitForSummaryUpdate(graph);
        summary.getFireflyStatistics(false);
        elementData = summary.getFireflyStatistics(true);
        Assert.assertTrue(elementData.vertexInfo.containsKey("lyndon"));
        Assert.assertEquals(6L, elementData.vertexInfo.get("lyndon").count);
        Assert.assertTrue(elementData.vertexInfo.get("lyndon").properties.containsAll(Set.of("foo1", "foo2", "foo3")));
        Assert.assertTrue(elementData.edgeInfo.containsKey("lyndon1"));
        Assert.assertEquals(5L, elementData.edgeInfo.get("lyndon1").count);
        Assert.assertTrue(elementData.edgeInfo.get("lyndon1").properties.containsAll(Set.of("foo1", "foo2", "foo3", "foo4")));
        Assert.assertTrue(elementData.supernodeInfo.containsKey("lyndon"));
        Assert.assertEquals(6L, elementData.supernodeInfo.get("lyndon").count);
    }

    @Test
    public void testSummaryDeprecatedWorks() throws InterruptedException {
        final GraphTraversalSource g = graph.traversal();
        g.V().drop().iterate();
        Thread.sleep(100);
        final Map<Object, Object> summaryCallEmpty = (Map<Object, Object>) g.call("summary").next();
        final Map<Object, Object> expectedEmpty = Map.of(
                "Vertex count by label", Map.of(),
                "Edge count by label", Map.of(),
                "Supernode count by label", Map.of(),
                "Edge properties by label", Map.of(),
                "Vertex properties by label", Map.of(),
                "Total vertex count", 0L,
                "Total edge count", 0L,
                "Total supernode count", 0L);
        Assert.assertEquals(expectedEmpty, summaryCallEmpty);
        GraphHelper.cloneElements(TinkerFactory.createGratefulDead(), graph);
        waitForSummaryUpdate(graph);
        final long vertexCount = g.V().count().next();
        final long edgeCount = g.E().count().next();
        final long supernodeCount = g.V()
                .filter(__.bothE().count().is(P.gt(graph.getBaseGraph().getConfig().onRecordIdLimit)))
                .count()
                .next();
        final Map<Object, Object> vertexLabels = g.V().group().by(__.label()).by(__.count()).next();
        final Map<Object, Object> edgeLabels = g.E().group().by(__.label()).by(__.count()).next();
        final Map<Object, Object> supernodeLabels = g.V()
                .filter(__.outE().count().is(P.gt(graph.getBaseGraph().getConfig().onRecordIdLimit)))
                .group()
                .by(__.label())
                .by(__.count()).next();
        final Map<Object, Object> vertexProperties = g.V().group().by(__.label()).by(__.properties().key().dedup().fold()).next();
        final Map<Object, Object> edgeProperties = g.E().group().by(__.label()).by(__.properties().key().dedup().fold()).next();
        for (final Object key : edgeLabels.keySet()) {
            if (!edgeProperties.containsKey(key)) {
                edgeProperties.put(key, List.of());
            }
        }
        for (final Object key : vertexProperties.keySet()) {
            if (!vertexProperties.containsKey(key)) {
                vertexProperties.put(key, List.of());
            }
        }
        vertexProperties.replaceAll((k, v) -> new HashSet<>((List<Object>) vertexProperties.get(k)));
        edgeProperties.replaceAll((k, v) -> new HashSet<>((List<Object>) edgeProperties.get(k)));

        final Map<Object, Object> expectedGrateful = Map.of(
                "Vertex count by label", vertexLabels,
                "Edge count by label", edgeLabels,
                "Supernode count by label", supernodeLabels,
                "Edge properties by label", edgeProperties,
                "Vertex properties by label", vertexProperties,
                "Total vertex count", vertexCount,
                "Total edge count", edgeCount,
                "Total supernode count", supernodeCount);
        final Map<Object, Object> summaryCallGrateful = (Map<Object, Object>) g.call("summary").next();
        Assert.assertEquals(expectedGrateful, summaryCallGrateful);
    }

    @Test
    public void testSummaryOverflow() throws InterruptedException {
        graph.traversal().V().drop().iterate();
        Thread.sleep(100);

        for (int i = 0; i < 10000; i++) {
            Vertex v = graph.traversal().addV(String.format("%d", i)).property(String.format("%d", i), String.format("%d", i)).next();
            graph.traversal().addE(String.format("%d", i)).from(v).to(v).property(String.format("%d", i), String.format("%d", i)).iterate();
        }
        waitForSummaryUpdate(graph);

        Assert.assertTrue(graph.fireflySummaryUpdater.vertexCounts.size() <= FireflyGraphSummaryUpdater.MAP_RECYCLE_SIZE);
        Assert.assertTrue(graph.fireflySummaryUpdater.vertexProperties.size() <= FireflyGraphSummaryUpdater.MAP_RECYCLE_SIZE);

        Assert.assertTrue(graph.fireflySummaryUpdater.edgeCounts.size() <= FireflyGraphSummaryUpdater.MAP_RECYCLE_SIZE);
        Assert.assertTrue(graph.fireflySummaryUpdater.edgeProperties.size() <= FireflyGraphSummaryUpdater.MAP_RECYCLE_SIZE);

        Assert.assertTrue(graph.fireflySummaryUpdater.supernodeCounts.size() <= FireflyGraphSummaryUpdater.MAP_RECYCLE_SIZE);

        graph.traversal().V().drop().iterate();
    }

    @Test
    public void testPrettySummary() throws InterruptedException {
        final GraphTraversalSource g = graph.traversal();
        g.V().drop().iterate();
        Thread.sleep(100);
        final String summaryCall = (String) g.call("aerospike.graph.admin.metadata.summary").with("pretty").next();
        final String expectedOutputEmpty = String.format(PRETTY_PRINT_FORMAT_SYSTEM, 0L, "{}", "{}", 0L, "{}", "{}", 0L, "{}");
        Assert.assertEquals(expectedOutputEmpty, summaryCall);
        GraphHelper.cloneElements(TinkerFactory.createGratefulDead(), graph);
        waitForSummaryUpdate(graph);
        final long vertexCount = g.V().count().next();
        final long edgeCount = g.E().count().next();
        final long supernodeCount = g.V()
                .filter(__.bothE().count().is(P.gt(graph.getBaseGraph().getConfig().onRecordIdLimit)))
                .count()
                .next();

        // Can't check properties in an automated way cause lists / sets / maps get reordered.
        // final Map<Object, Object> vertexLabels = g.V().group().by(__.label()).by(__.count()).next();
        // final Map<Object, Object> edgeLabels = g.E().group().by(__.label()).by(__.count()).next();
        // final Map<Object, Object> vertexProperties = g.V().group().by(__.label()).by(__.properties().key().dedup().fold()).next();
        // final Map<Object, Object> edgeProperties = g.E().group().by(__.label()).by(__.properties().key().dedup().fold()).next();

        final String actualOutputGrateful = (String) g.call("aerospike.graph.admin.metadata.summary").with("pretty").next();
        final String[] gratefulDelimiterSplit = actualOutputGrateful.split("\n");
        for (int i = 0; i < gratefulDelimiterSplit.length; i += 2) {
            if (gratefulDelimiterSplit[i].contains("Total vertex count:")) {
                final String totalVertexCountString = gratefulDelimiterSplit[i].split("Total vertex count:")[1].trim();
                Assert.assertEquals(vertexCount, Long.parseLong(totalVertexCountString.substring(0, totalVertexCountString.length() - 1)));
            } else if (gratefulDelimiterSplit[i].contains("Total edge count:")) {
                final String totalEdgeCountString = gratefulDelimiterSplit[i].split("Total edge count:")[1].trim();
                Assert.assertEquals(vertexCount, Long.parseLong(totalEdgeCountString.substring(0, totalEdgeCountString.length() - 1)));
                Assert.assertEquals(edgeCount, Long.parseLong(gratefulDelimiterSplit[i].split("Total edge count:")[1].trim()));
            } else if (gratefulDelimiterSplit[i].contains("Total supernode count:")) {
                final String totalSupernodeCountString = gratefulDelimiterSplit[i].split("Total supernode count:")[1].trim();
                Assert.assertEquals(supernodeCount, Long.parseLong(totalSupernodeCountString.substring(0, totalSupernodeCountString.length() - 1)));
            }
        }
    }

    @Test
    public void testSupernodeCountOltp() throws InterruptedException {
        final GraphTraversalSource g = graph.traversal();
        g.V().drop().iterate();
        Thread.sleep(100);
        final Map<Object, Object> summaryCallEmpty = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();
        final Map<Object, Object> expectedEmpty = Map.of(
                "Vertex count by label", Map.of(),
                "Edge count by label", Map.of(),
                "Supernode count by label", Map.of(),
                "Edge properties by label", Map.of(),
                "Vertex properties by label", Map.of(),
                "Total vertex count", 0L,
                "Total edge count", 0L,
                "Total supernode count", 0L);
        Assert.assertEquals(expectedEmpty, summaryCallEmpty);

        // Create 3 vertices
        Vertex v1 = g.addV("person").property("name", "v1").next();
        Vertex v2 = g.addV("movie").property("name", "v2").next();
        Vertex v3 = g.addV("movie").property("name", "v3").next();

        // Add many edges from v1 to v2, to make sure they are considered a supernode
        for (int i = 0; i < 10000; i++) {
            v1.addEdge("knows", v2, "edgeId", i);
        }

        // Add a single edge from v3 to v2
        v3.addEdge("knows", v2, "edgeId", "solo");

        waitForSummaryUpdate(graph);

        final long vertexCount = g.V().count().next();
        final long edgeCount = g.E().count().next();
        final long supernodeCount = g.V()
                .filter(__.bothE().count().is(P.gt(graph.getBaseGraph().getConfig().onRecordIdLimit)))
                .count()
                .next();

        final Map<Object, Object> vertexLabels = g.V().group().by(__.label()).by(__.count()).next();
        final Map<Object, Object> edgeLabels = g.E().group().by(__.label()).by(__.count()).next();
        final Map<Object, Object> supernodeLabels = g.V()
                .filter(__.bothE().count().is(P.gt(graph.getBaseGraph().getConfig().onRecordIdLimit)))
                .group()
                .by(__.label())
                .by(__.count())
                .next();

        Map<Object, Object> summaryCallGrateful = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();
        Assert.assertEquals(vertexLabels, summaryCallGrateful.get("Vertex count by label"));
        Assert.assertEquals(edgeLabels, summaryCallGrateful.get("Edge count by label"));
        Assert.assertEquals(supernodeLabels, summaryCallGrateful.get("Supernode count by label"));
        Assert.assertEquals(3L, vertexCount);
        Assert.assertEquals(vertexCount, summaryCallGrateful.get("Total vertex count"));
        Assert.assertEquals(10001L, edgeCount);
        Assert.assertEquals(edgeCount, summaryCallGrateful.get("Total edge count"));
        Assert.assertEquals(2, supernodeCount);
        Assert.assertEquals(supernodeCount, summaryCallGrateful.get("Total supernode count"));

        // Delete vertex to test if supernode count decreases
        v1.remove();

        waitForSummaryUpdate(graph);

        summaryCallGrateful = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();
        Assert.assertEquals(2L, ((Map<Object, Object>) summaryCallGrateful.get("Vertex count by label")).get("movie"));
        Assert.assertEquals(0L, ((Map<Object, Object>) summaryCallGrateful.get("Vertex count by label")).get("person"));
        Assert.assertEquals(1L, ((Map<Object, Object>) summaryCallGrateful.get("Edge count by label")).get("knows"));
        // It's still 1 (and not 0) because we are not "demoting" supernodes (1 was removed due to vertex deletion).
        Assert.assertEquals(1L, ((Map<Object, Object>) summaryCallGrateful.get("Supernode count by label")).get("movie"));
        Assert.assertEquals(0L, ((Map<Object, Object>) summaryCallGrateful.get("Supernode count by label")).get("person"));
        Assert.assertEquals(2L, summaryCallGrateful.get("Total vertex count"));
        Assert.assertEquals(1L, summaryCallGrateful.get("Total edge count"));
        // It's still 1 (and not 0) because we are not "demoting" supernodes (1 was removed due to vertex deletion).
        Assert.assertEquals(1L, summaryCallGrateful.get("Total supernode count"));
    }

    private void waitForSummaryUpdate(final FireflyGraph graph) {
        graph.fireflySummaryUpdater.forceWrite();
        // Wait an extra second after forcing write
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
