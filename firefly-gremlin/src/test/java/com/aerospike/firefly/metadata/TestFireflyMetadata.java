package com.aerospike.firefly.metadata;

import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.firefly.io.FireflyCardinalityMetadata;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class TestFireflyMetadata extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return false;
    }

    int getInitialIndexCount() {
        final String integrationTestProperties = System.getProperty("integration.test.properties");
        if (integrationTestProperties != null && integrationTestProperties.endsWith("-sindex")) {
            return 14;
        } else {
            return 2;
        }
    }

    @Before
    public void clearIndexes() {
        db.clearNamespace();
    }

    @Ignore //@todo
    @Test
    public void testFireflyIndexMetadata() throws InterruptedException {
        // Nuke any existing indexes.
        graph.getBaseGraph().clearNamespace();

        // Set metadata to update every millisecond for this test.
        config.setProperty(ConfigurationHelper.Keys.INDEX_METADATA_UPDATE_FREQUENCY.toLowerCase(), "1");
        graph.close();
        graph = FireflyGraph.open(config);

        // Check there are no indexes initially (besides vertex and edge label indexes).
        Thread.sleep(1000);
        //@todo
        Assert.assertEquals(getInitialIndexCount(), graph.fireflyIndexMetadata.getPropertyIndexInfos().size());

        // Create 'index1'.
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES.toLowerCase(), "index1");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
        }

        // Check for 'index1'. Need to check for both string and numeric existence.
        Thread.sleep(10);
        Assert.assertEquals(getInitialIndexCount() + 2, graph.fireflyIndexMetadata.getPropertyIndexInfos().size());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo("index1", 1L).isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo("index1", "1").isPresent());

        // Create 'index2' and leave 'index1' there.
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES.toLowerCase(), "index1, index2");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
        }

        // Check for 'index1' and 'index2'. Need to check for both string and numeric existence.
        Thread.sleep(10);
        Assert.assertEquals(getInitialIndexCount() + 4, graph.fireflyIndexMetadata.getPropertyIndexInfos().size());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo("index1", 1L).isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo("index1", "1").isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo("index2", 1L).isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo("index2", "1").isPresent());

        // Create 'index3' and remove 'index1' and 'index2'.
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES.toLowerCase(), "index3");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
        }

        // Check for 'index1', 'index2', and 'index3'. Need to check for both string and numeric existence.
        Thread.sleep(10);
        Assert.assertEquals(getInitialIndexCount() + 6, graph.fireflyIndexMetadata.getPropertyIndexInfos().size());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo("index1", 1L).isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo("index1", "1").isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo("index2", 1L).isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo("index2", "1").isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo("index3", 1L).isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo("index3", "1").isPresent());

        // Forcibly remove index 1.
        final FireflyIndexMetadata.IndexInfo index1InfoLong = graph.fireflyIndexMetadata.getPropertyIndexInfo("index1", 1L).get();
        final FireflyIndexMetadata.IndexInfo index1InfoString = graph.fireflyIndexMetadata.getPropertyIndexInfo("index1", "1").get();

        db.getClient().dropIndex(new QueryPolicy(), db.getNamespace(), db.VERTEX_AERO_SET, index1InfoLong.indexName);
        db.getClient().dropIndex(new QueryPolicy(), db.getNamespace(), db.VERTEX_AERO_SET, index1InfoString.indexName);

        // Check for 'index1' (should be removed), 'index2', and 'index3'. Need to check for both string and numeric existence.
        Thread.sleep(10);
        Assert.assertEquals(getInitialIndexCount() + 4, graph.fireflyIndexMetadata.getPropertyIndexInfos().size());
        Assert.assertFalse(graph.fireflyIndexMetadata.getPropertyIndexInfo("index1", 1L).isPresent());
        Assert.assertFalse(graph.fireflyIndexMetadata.getPropertyIndexInfo("index1", "1").isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo("index2", 1L).isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo("index2", "1").isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo("index3", 1L).isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo("index3", "1").isPresent());

        // Forcibly remove 'index2' and 'index3'.
        final FireflyIndexMetadata.IndexInfo index2InfoLong = graph.fireflyIndexMetadata.getPropertyIndexInfo("index2", 1L).get();
        final FireflyIndexMetadata.IndexInfo index2InfoString = graph.fireflyIndexMetadata.getPropertyIndexInfo("index2", "1").get();
        final FireflyIndexMetadata.IndexInfo index3InfoLong = graph.fireflyIndexMetadata.getPropertyIndexInfo("index3", 1L).get();
        final FireflyIndexMetadata.IndexInfo index3InfoString = graph.fireflyIndexMetadata.getPropertyIndexInfo("index3", "1").get();

        db.getClient().dropIndex(new QueryPolicy(), db.getNamespace(), db.VERTEX_AERO_SET, index2InfoLong.indexName);
        db.getClient().dropIndex(new QueryPolicy(), db.getNamespace(), db.VERTEX_AERO_SET, index2InfoString.indexName);
        db.getClient().dropIndex(new QueryPolicy(), db.getNamespace(), db.VERTEX_AERO_SET, index3InfoLong.indexName);
        db.getClient().dropIndex(new QueryPolicy(), db.getNamespace(), db.VERTEX_AERO_SET, index3InfoString.indexName);

        // Verify all indexes are removed.
        Thread.sleep(10);
        Assert.assertEquals(getInitialIndexCount(), graph.fireflyIndexMetadata.getPropertyIndexInfos().size());
        Assert.assertFalse(graph.fireflyIndexMetadata.getPropertyIndexInfo("index1", 1L).isPresent());
        Assert.assertFalse(graph.fireflyIndexMetadata.getPropertyIndexInfo("index1", "1").isPresent());
        Assert.assertFalse(graph.fireflyIndexMetadata.getPropertyIndexInfo("index2", 1L).isPresent());
        Assert.assertFalse(graph.fireflyIndexMetadata.getPropertyIndexInfo("index2", "1").isPresent());
        Assert.assertFalse(graph.fireflyIndexMetadata.getPropertyIndexInfo("index3", 1L).isPresent());
        Assert.assertFalse(graph.fireflyIndexMetadata.getPropertyIndexInfo("index3", "1").isPresent());
    }

    @Test
    public void testFireflyCardinalityMetadata() throws InterruptedException {
        // Set metadata to update every millisecond for this test.
        db.conf.setProperty(ConfigurationHelper.Keys.ENABLE_PERIODIC_CARDINALITY_METADATA_UPDATE.toLowerCase(), true);
        db.conf.setProperty(ConfigurationHelper.Keys.CARDINALITY_METADATA_UPDATE_FREQUENCY.toLowerCase(), "5");
        graph.close();
        graph = FireflyGraph.open(db.conf);
        db.conf.setProperty(ConfigurationHelper.Keys.ENABLE_PERIODIC_CARDINALITY_METADATA_UPDATE.toLowerCase(), false);

        final Vertex a = graph.traversal().addV("label1").property("key1", "value1").next();
        final Vertex b = graph.traversal().addV("label1").property("key1", "value1").next();
        graph.traversal().addV("label1").property("key2", "value2").iterate();
        graph.traversal().addV("label2").property("key2", "value2").iterate();
        graph.traversal().addV("label2").property("key3", "value3").iterate();
        graph.traversal().addV("label2").property("key3", "value3").iterate();
        graph.traversal().addV("label3").property("key4", "value4").iterate();
        graph.traversal().addV("label3").property("key4", "value4").iterate();
        graph.traversal().addV("label3").property("key5", "value5").iterate();
        graph.traversal().addV("label3").property("key5", "value5").iterate();
        graph.traversal().addV("label3").property("key4", 1).iterate();
        graph.traversal().addV("label3").property("key5", 2).iterate();

        graph.traversal().addE("edgeLabel1").from(a).to(b).property("edgeKey1", "edgeValue1").iterate();
        graph.traversal().addE("edgeLabel1").from(a).to(b).property("edgeKey1", "edgeValue1").iterate();
        graph.traversal().addE("edgeLabel2").from(a).to(b).property("edgeKey2", "edgeValue2").iterate();
        graph.traversal().addE("edgeLabel2").from(a).to(b).property("edgeKey2", "edgeValue2").iterate();
        graph.traversal().addE("edgeLabel3").from(a).to(b).property("edgeKey3", 3).iterate();
        graph.traversal().addE("edgeLabel3").from(a).to(b).property("edgeKey3", 3).iterate();

        // Force an update on the server
        db.dropGraphIndices();
        db.createGraphIndexes();

        Thread.sleep(10);
        final FireflyCardinalityMetadata.CardinalityInfo vertexLabelCardinalityInfo = graph.fireflyCardinalityMetadata.vertexLabelCardinalityInfo;
        final FireflyCardinalityMetadata.CardinalityInfo vertexStringPropertyCardinalityInfo = graph.fireflyCardinalityMetadata.vertexStringPropertyCardinalityInfo;
        final FireflyCardinalityMetadata.CardinalityInfo vertexNumericPropertyCardinalityInfo = graph.fireflyCardinalityMetadata.vertexNumericPropertyCardinalityInfo;
        final FireflyCardinalityMetadata.CardinalityInfo edgeLabelCardinalityInfo = graph.fireflyCardinalityMetadata.edgeLabelCardinalityInfo;
        final FireflyCardinalityMetadata.CardinalityInfo edgeStringPropertyCardinalityInfo = graph.fireflyCardinalityMetadata.edgeStringPropertyCardinalityInfo;
        final FireflyCardinalityMetadata.CardinalityInfo edgeNumericPropertyCardinalityInfo = graph.fireflyCardinalityMetadata.edgeNumericPropertyCardinalityInfo;

        // 12 vertices, 3 unique labels, 5 unique keys, 5 unique string values, 10 total string values, 2 unique numeric values, 5 total numeric values
        Assert.assertTrue(vertexLabelCardinalityInfo.valid);
        Assert.assertTrue(vertexStringPropertyCardinalityInfo.valid);
        Assert.assertTrue(vertexNumericPropertyCardinalityInfo.valid);

        Assert.assertNotNull(vertexLabelCardinalityInfo.totalEntries);
        Assert.assertNotNull(vertexStringPropertyCardinalityInfo.totalEntries);
        Assert.assertNotNull(vertexNumericPropertyCardinalityInfo.totalEntries);

        Assert.assertEquals(12L, vertexLabelCardinalityInfo.totalEntries.longValue());
        Assert.assertEquals(10L, vertexStringPropertyCardinalityInfo.totalEntries.longValue());
        Assert.assertEquals(2L, vertexNumericPropertyCardinalityInfo.totalEntries.longValue());

        Assert.assertNotNull(vertexLabelCardinalityInfo.entriesPerBval);
        Assert.assertNotNull(vertexStringPropertyCardinalityInfo.entriesPerBval);
        Assert.assertNotNull(vertexNumericPropertyCardinalityInfo.entriesPerBval);

        Assert.assertEquals(12L / 3L, vertexLabelCardinalityInfo.entriesPerBval.longValue());
        Assert.assertEquals(10L / 5L, vertexStringPropertyCardinalityInfo.entriesPerBval.longValue());
        Assert.assertEquals(1L, vertexNumericPropertyCardinalityInfo.entriesPerBval.longValue());

        // 6 edges, 3 unique labels, 3 unique keys, 2 unique string values, 1 unique numeric values.
        Assert.assertTrue(edgeLabelCardinalityInfo.valid);
        Assert.assertTrue(edgeStringPropertyCardinalityInfo.valid);
        Assert.assertTrue(edgeNumericPropertyCardinalityInfo.valid);

        Assert.assertNotNull(edgeLabelCardinalityInfo.totalEntries);
        Assert.assertNotNull(edgeStringPropertyCardinalityInfo.totalEntries);
        Assert.assertNotNull(edgeNumericPropertyCardinalityInfo.totalEntries);

        Assert.assertEquals(6L, edgeLabelCardinalityInfo.totalEntries.longValue());
        Assert.assertEquals(4L, edgeStringPropertyCardinalityInfo.totalEntries.longValue());
        Assert.assertEquals(2L, edgeNumericPropertyCardinalityInfo.totalEntries.longValue());

        Assert.assertNotNull(edgeLabelCardinalityInfo.entriesPerBval);
        Assert.assertNotNull(edgeStringPropertyCardinalityInfo.entriesPerBval);
        Assert.assertNotNull(edgeNumericPropertyCardinalityInfo.entriesPerBval);

        Assert.assertEquals(6L / 3L, edgeLabelCardinalityInfo.entriesPerBval.longValue());
        Assert.assertEquals(4L / 2L, edgeStringPropertyCardinalityInfo.entriesPerBval.longValue());
        Assert.assertEquals(2L, edgeNumericPropertyCardinalityInfo.entriesPerBval.longValue());
    }
}
