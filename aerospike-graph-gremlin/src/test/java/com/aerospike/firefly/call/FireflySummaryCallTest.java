package com.aerospike.firefly.call;

import com.aerospike.firefly.runtime.tasks.FireflyGraphSummaryUpdater;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.GraphHelper;
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
        expectedConfig.put("Unified Configuration", "Not available");

        final GraphTraversalSource g = graph.traversal();
        final Map<String, String> config = (Map<String, String>) g.call("aerospike.graph.admin.metadata.config").next();
        Assert.assertEquals(expectedConfig, config);
    }

    @Test
    public void testSummary() throws InterruptedException {
        final GraphTraversalSource g = graph.traversal();
        Thread.sleep(5000);
        g.V().drop().iterate();
        Thread.sleep(5000);
        final Map<Object, Object> summaryCallEmpty = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();
        final Map<Object, Object> expectedEmpty = Map.of(
                "Vertex count by label", Map.of(),
                "Edge count by label", Map.of(),
                "Edge properties by label", Map.of(),
                "Vertex properties by label", Map.of(),
                "Total vertex count", 0L,
                "Total edge count", 0L);
        Assert.assertEquals(expectedEmpty, summaryCallEmpty);
        GraphHelper.cloneElements(TinkerFactory.createGratefulDead(), graph);
        Thread.sleep(3000);
        final long vertexCount = g.V().count().next();
        final long edgeCount = g.E().count().next();
        final Map<Object, Object> vertexLabels = g.V().group().by(__.label()).by(__.count()).next();
        final Map<Object, Object> edgeLabels = g.E().group().by(__.label()).by(__.count()).next();
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
        for (final Object key : vertexProperties.keySet()) {
            vertexProperties.put(key, new HashSet((List<Object>) vertexProperties.get(key)));
        }
        for (final Object key : edgeProperties.keySet()) {
            edgeProperties.put(key, new HashSet((List<Object>) edgeProperties.get(key)));
        }
        final Map<Object, Object> expectedGrateful = Map.of(
                        "Vertex count by label", vertexLabels,
                        "Edge count by label", edgeLabels,
                        "Edge properties by label", edgeProperties,
                        "Vertex properties by label", vertexProperties,
                        "Total vertex count", vertexCount,
                        "Total edge count", edgeCount);
        final Map<Object, Object> summaryCallGrateful = (Map<Object, Object>) g.call("aerospike.graph.admin.metadata.summary").next();
        Assert.assertEquals(expectedGrateful, summaryCallGrateful);
    }

    @Test
    public void testSummaryDeprecatedWorks() throws InterruptedException {
        final GraphTraversalSource g = graph.traversal();
        Thread.sleep(5000);
        g.V().drop().iterate();
        Thread.sleep(5000);
        final Map<Object, Object> summaryCallEmpty = (Map<Object, Object>) g.call("summary").next();
        final Map<Object, Object> expectedEmpty = Map.of(
                "Vertex count by label", Map.of(),
                "Edge count by label", Map.of(),
                "Edge properties by label", Map.of(),
                "Vertex properties by label", Map.of(),
                "Total vertex count", 0L,
                "Total edge count", 0L);
        Assert.assertEquals(expectedEmpty, summaryCallEmpty);
        GraphHelper.cloneElements(TinkerFactory.createGratefulDead(), graph);
        Thread.sleep(3000);
        final long vertexCount = g.V().count().next();
        final long edgeCount = g.E().count().next();
        final Map<Object, Object> vertexLabels = g.V().group().by(__.label()).by(__.count()).next();
        final Map<Object, Object> edgeLabels = g.E().group().by(__.label()).by(__.count()).next();
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
        for (final Object key : vertexProperties.keySet()) {
            vertexProperties.put(key, new HashSet((List<Object>) vertexProperties.get(key)));
        }
        for (final Object key : edgeProperties.keySet()) {
            edgeProperties.put(key, new HashSet((List<Object>) edgeProperties.get(key)));
        }
        final Map<Object, Object> expectedGrateful = Map.of(
                "Vertex count by label", vertexLabels,
                "Edge count by label", edgeLabels,
                "Edge properties by label", edgeProperties,
                "Vertex properties by label", vertexProperties,
                "Total vertex count", vertexCount,
                "Total edge count", edgeCount);
        final Map<Object, Object> summaryCallGrateful = (Map<Object, Object>) g.call("summary").next();
        Assert.assertEquals(expectedGrateful, summaryCallGrateful);
    }

    @Test
    public void testSummaryOverflow() throws InterruptedException {
        graph.traversal().V().drop().iterate();
        Thread.sleep(5000);

        for (int i = 0; i < 10000; i++) {
            Vertex v = graph.traversal().addV(String.format("%d", i)).property(String.format("%d", i), String.format("%d", i)).next();
            graph.traversal().addE(String.format("%d", i)).from(v).to(v).property(String.format("%d", i), String.format("%d", i)).iterate();
        }

        // Sleep 5 seconds to allow recycle to trigger.
        Thread.sleep(5000);

        Assert.assertTrue(FireflyGraphSummaryUpdater.vertexCounts.size() <= FireflyGraphSummaryUpdater.MAP_RECYCLE_SIZE);
        Assert.assertTrue(FireflyGraphSummaryUpdater.vertexProperties.size() <= FireflyGraphSummaryUpdater.MAP_RECYCLE_SIZE);

        Assert.assertTrue(FireflyGraphSummaryUpdater.edgeCounts.size() <= FireflyGraphSummaryUpdater.MAP_RECYCLE_SIZE);
        Assert.assertTrue(FireflyGraphSummaryUpdater.edgeProperties.size() <= FireflyGraphSummaryUpdater.MAP_RECYCLE_SIZE);

        graph.traversal().V().drop().iterate();
    }

    @Test
    public void testPrettySummary() throws InterruptedException {
        final GraphTraversalSource g = graph.traversal();
        Thread.sleep(5000);
        g.V().drop().iterate();
        Thread.sleep(5000);
        final String summaryCall = (String) g.call("aerospike.graph.admin.metadata.summary").with("pretty").next();
        final String expectedOutputEmpty = String.format(PRETTY_PRINT_FORMAT_SYSTEM, 0L, "{}", "{}", 0L, "{}", "{}");
        Assert.assertEquals(expectedOutputEmpty, summaryCall);
        GraphHelper.cloneElements(TinkerFactory.createGratefulDead(), graph);
        Thread.sleep(5000);
        final long vertexCount = g.V().count().next();
        final long edgeCount = g.E().count().next();

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
            }
        }
    }
}
