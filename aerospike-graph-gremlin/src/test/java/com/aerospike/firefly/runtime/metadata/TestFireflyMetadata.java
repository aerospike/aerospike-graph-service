package com.aerospike.firefly.runtime.metadata;

import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.IndexType;
import com.aerospike.firefly.io.FireflyCardinalityMetadata;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.util.Optional;

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

    @Override
    protected boolean runTest() {
        // This test is only valid on one node clusters due to how the calculations for cardinality work.
        return db.getClient().getNodes().length == 1;
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
        Thread.sleep(10);
        Assert.assertEquals(getInitialIndexCount(), graph.fireflyIndexMetadata.getPropertyIndexInfos().size());

        // Create 'index1'.
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES.toLowerCase(), "index1");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
        }

        // Check for 'index1'. Need to check for both string and numeric existence.
        Thread.sleep(10);
        Assert.assertEquals(getInitialIndexCount() + 2, graph.fireflyIndexMetadata.getPropertyIndexInfos().size());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index1", 1L).isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index1", "1").isPresent());

        // Create 'index2' and leave 'index1' there.
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES.toLowerCase(), "index1, index2");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
        }

        // Check for 'index1' and 'index2'. Need to check for both string and numeric existence.
        Thread.sleep(10);
        Assert.assertEquals(getInitialIndexCount() + 4, graph.fireflyIndexMetadata.getPropertyIndexInfos().size());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index1", 1L).isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index1", "1").isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index2", 1L).isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index2", "1").isPresent());

        // Create 'index3' and remove 'index1' and 'index2'.
        config.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES.toLowerCase(), "index3");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
        }

        // Check for 'index1', 'index2', and 'index3'. Need to check for both string and numeric existence.
        Thread.sleep(10);
        Assert.assertEquals(getInitialIndexCount() + 6, graph.fireflyIndexMetadata.getPropertyIndexInfos().size());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index1", 1L).isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index1", "1").isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index2", 1L).isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index2", "1").isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index3", 1L).isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index3", "1").isPresent());

        // Forcibly remove index 1.
        final FireflyIndexMetadata.IndexInfo index1InfoLong = graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index1", 1L).get();
        final FireflyIndexMetadata.IndexInfo index1InfoString = graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index1", "1").get();

        db.getClient().dropIndex(new QueryPolicy(), db.getNamespace(), db.VERTEX_AERO_SET, index1InfoLong.indexName);
        db.getClient().dropIndex(new QueryPolicy(), db.getNamespace(), db.VERTEX_AERO_SET, index1InfoString.indexName);

        // Check for 'index1' (should be removed), 'index2', and 'index3'. Need to check for both string and numeric existence.
        Thread.sleep(10);
        Assert.assertEquals(getInitialIndexCount() + 4, graph.fireflyIndexMetadata.getPropertyIndexInfos().size());
        Assert.assertFalse(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index1", 1L).isPresent());
        Assert.assertFalse(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index1", "1").isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index2", 1L).isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index2", "1").isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index3", 1L).isPresent());
        Assert.assertTrue(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index3", "1").isPresent());

        // Forcibly remove 'index2' and 'index3'.
        final FireflyIndexMetadata.IndexInfo index2InfoLong = graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index2", 1L).get();
        final FireflyIndexMetadata.IndexInfo index2InfoString = graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index2", "1").get();
        final FireflyIndexMetadata.IndexInfo index3InfoLong = graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index3", 1L).get();
        final FireflyIndexMetadata.IndexInfo index3InfoString = graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index3", "1").get();

        db.getClient().dropIndex(new QueryPolicy(), db.getNamespace(), db.VERTEX_AERO_SET, index2InfoLong.indexName);
        db.getClient().dropIndex(new QueryPolicy(), db.getNamespace(), db.VERTEX_AERO_SET, index2InfoString.indexName);
        db.getClient().dropIndex(new QueryPolicy(), db.getNamespace(), db.VERTEX_AERO_SET, index3InfoLong.indexName);
        db.getClient().dropIndex(new QueryPolicy(), db.getNamespace(), db.VERTEX_AERO_SET, index3InfoString.indexName);

        // Verify all indexes are removed.
        Thread.sleep(10);
        Assert.assertEquals(getInitialIndexCount(), graph.fireflyIndexMetadata.getPropertyIndexInfos().size());
        Assert.assertFalse(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index1", 1L).isPresent());
        Assert.assertFalse(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index1", "1").isPresent());
        Assert.assertFalse(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index2", 1L).isPresent());
        Assert.assertFalse(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index2", "1").isPresent());
        Assert.assertFalse(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index3", 1L).isPresent());
        Assert.assertFalse(graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "index3", "1").isPresent());
    }

    @Test
    public void testFireflyCardinalityMetadata() throws InterruptedException {
        // Set metadata to update every millisecond for this test.
        db.conf.setProperty(ConfigurationHelper.Keys.CARDINALITY_METADATA_UPDATE_FREQUENCY.toLowerCase(), "5");
        db.conf.setProperty(ConfigurationHelper.Keys.INDEX_METADATA_UPDATE_FREQUENCY.toLowerCase(), "1");
        final String originalIndexes = (String) db.conf.getProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES.toLowerCase());
        db.conf.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES.toLowerCase(), "key1,key2,key3,key4,key5");
        db.conf.setProperty(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), true);
        // TODO GRAPH-442: Enable this when edge label indexes are supported.
        db.conf.setProperty(ConfigurationHelper.Keys.E_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), false);

        graph.close();
        graph = FireflyGraph.open(db.conf);

        // Remove all indexes.
        db.clearNamespace();

        // Insert data.
        final Vertex a = graph.traversal().addV("label1").property("key1", "value1").next();
        final Vertex b = graph.traversal().addV("label1").property("key1", "value2").next();
        graph.traversal().addV("label1").property("key2", 1).iterate();
        graph.traversal().addV("label2").property("key2", 2).iterate();
        graph.traversal().addV("label2").property("key3", "value1").iterate();
        graph.traversal().addV("label2").property("key3", 2).iterate();
        graph.traversal().addV("label3").property("key4", "value1").iterate();
        graph.traversal().addV("label3").property("key4", "value1").iterate();
        graph.traversal().addV("label3").property("key4", 1).iterate();
        graph.traversal().addV("label3").property("key5", 1).iterate();
        graph.traversal().addV("label3").property("key5", 1).iterate();
        graph.traversal().addV("label3").property("key5", 1L).iterate();

        graph.traversal().addE("edgeLabel1").from(a).to(b).property("edgeKey1", "edgeValue1").iterate();
        graph.traversal().addE("edgeLabel1").from(a).to(b).property("edgeKey1", "edgeValue1").iterate();
        graph.traversal().addE("edgeLabel2").from(a).to(b).property("edgeKey2", "edgeValue2").iterate();
        graph.traversal().addE("edgeLabel2").from(a).to(b).property("edgeKey2", "edgeValue2").iterate();
        graph.traversal().addE("edgeLabel3").from(a).to(b).property("edgeKey3", 3).iterate();
        graph.traversal().addE("edgeLabel3").from(a).to(b).property("edgeKey3", 3).iterate();

        // Force Firefly to rebuild the indexes.
        graph.close();
        graph = FireflyGraph.open(db.conf);

        db.conf.setProperty(ConfigurationHelper.Keys.VERTEX_PROPERTY_INDEXES.toLowerCase(), originalIndexes);

        Thread.sleep(10);

        // Get vertex label cardinality optional and grab the cardinality info attached.
        final Optional<FireflyCardinalityMetadata.CardinalityInfo> vertexLabelCardinalityInfoOptional =
                graph.fireflyCardinalityMetadata.getVertexLabelCardinality();
        Assert.assertTrue(vertexLabelCardinalityInfoOptional.isPresent());
        final FireflyCardinalityMetadata.CardinalityInfo vertexLabelCardinalityInfo = vertexLabelCardinalityInfoOptional.get();

        // Validate vertex label cardinality. 12 vertices, 3 unique labels.
        Assert.assertTrue(vertexLabelCardinalityInfo.valid);
        Assert.assertNotNull(vertexLabelCardinalityInfo.totalEntries);
        Assert.assertNotNull(vertexLabelCardinalityInfo.entriesPerBval);
        Assert.assertEquals(12L, vertexLabelCardinalityInfo.totalEntries.longValue());
        Assert.assertEquals(12L / 3L, vertexLabelCardinalityInfo.entriesPerBval.longValue());

        // Get edge label cardinality optional and grab the cardinality info attached.
//        final Optional<FireflyCardinalityMetadata.CardinalityInfo> edgeLabelCardinalityInfoOptional =
//                graph.fireflyCardinalityMetadata.getEdgeLabelCardinality();
//        Assert.assertTrue(edgeLabelCardinalityInfoOptional.isPresent());
//        final FireflyCardinalityMetadata.CardinalityInfo edgeLabelCardinalityInfo = edgeLabelCardinalityInfoOptional.get();

        // Validate edge label cardinality. 6 edges, 3 unique labels.
//        Assert.assertTrue(edgeLabelCardinalityInfo.valid);
//        Assert.assertNotNull(edgeLabelCardinalityInfo.totalEntries);
//        Assert.assertNotNull(edgeLabelCardinalityInfo.entriesPerBval);
//        Assert.assertEquals(6L, edgeLabelCardinalityInfo.totalEntries.longValue());
//        Assert.assertEquals(6L / 3L, edgeLabelCardinalityInfo.entriesPerBval.longValue());

        validateVertexPropertyMetadata(graph.fireflyCardinalityMetadata, "key1", IndexType.STRING, 2L, 2L);
        validateVertexPropertyMetadata(graph.fireflyCardinalityMetadata, "key1", IndexType.NUMERIC, 0L, 0L);
        validateVertexPropertyMetadata(graph.fireflyCardinalityMetadata, "key2", IndexType.STRING, 0L, 0L);
        validateVertexPropertyMetadata(graph.fireflyCardinalityMetadata, "key2", IndexType.NUMERIC, 2L, 2L);
        validateVertexPropertyMetadata(graph.fireflyCardinalityMetadata, "key3", IndexType.STRING, 1L, 1L);
        validateVertexPropertyMetadata(graph.fireflyCardinalityMetadata, "key3", IndexType.NUMERIC, 1L, 1L);
        validateVertexPropertyMetadata(graph.fireflyCardinalityMetadata, "key4", IndexType.STRING, 2L, 1L);
        validateVertexPropertyMetadata(graph.fireflyCardinalityMetadata, "key4", IndexType.NUMERIC, 1L, 1L);
        validateVertexPropertyMetadata(graph.fireflyCardinalityMetadata, "key5", IndexType.STRING, 0L, 0L);
        validateVertexPropertyMetadata(graph.fireflyCardinalityMetadata, "key5", IndexType.NUMERIC, 3L, 1L);

        graph.getBaseGraph().clearNamespace();
    }

    private static void validateVertexPropertyMetadata(final FireflyCardinalityMetadata cardinalityMetadata, final String key, final IndexType indexType, final Long totalEntries, final Long entriesPerBval) {
        final Optional<FireflyCardinalityMetadata.CardinalityInfo> cardinalityInfoOptional = cardinalityMetadata.getVertexPropertyCardinality(key, indexType);
        Assert.assertTrue(cardinalityInfoOptional.isPresent());
        final FireflyCardinalityMetadata.CardinalityInfo cardinalityInfo = cardinalityInfoOptional.get();
        Assert.assertNotNull(cardinalityInfo);
        Assert.assertEquals((entriesPerBval > 0), cardinalityInfo.valid);
        Assert.assertNotNull(cardinalityInfo.totalEntries);
        Assert.assertNotNull(cardinalityInfo.entriesPerBval);
        Assert.assertEquals(totalEntries.longValue(), cardinalityInfo.totalEntries.longValue());
        Assert.assertEquals(entriesPerBval == 0 ? 0 : totalEntries / entriesPerBval, cardinalityInfo.entriesPerBval.longValue());
    }
}
