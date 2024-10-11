package com.aerospike.firefly.io.aerospike.indexes;

import com.aerospike.client.query.IndexType;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.query.GraphQuery;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestFireflyVertexIndexes extends TestFireflyIndexes {
    @Override
    protected void setProperty(final String propertyList) {
        config.setProperty("aerospike.graph.index.vertex.properties", propertyList);
    }

    @Override
    protected String getIndexPrefix() {
        return db.getVpIndexPrefix();
    }

    @Override
    protected Optional<FireflyIndexMetadata.IndexInfo> getPropertyIndexInfo(final FireflyGraph fireflyGraph, final String key, final Object value) {
        final List<String> indexes = fireflyGraph.fireflyIndexMetadata.getIndexesInProgress();
        for (final String index : indexes) {
            if (index.startsWith(fireflyGraph.getBaseGraph().getVpIndexPrefix())) {
                if (index.contains(key)) {
                    if (value instanceof String && index.contains(IndexType.STRING.toString())) {
                        return Optional.of(new FireflyIndexMetadata.IndexInfo(index, key, IndexType.STRING, fireflyGraph.getBaseGraph().VERTEX_AERO_SET));
                    } else if (value instanceof Number && index.contains(IndexType.NUMERIC.toString())) {
                        return Optional.of(new FireflyIndexMetadata.IndexInfo(index, key, IndexType.NUMERIC, fireflyGraph.getBaseGraph().VERTEX_AERO_SET));
                    }
                }
            }
        }
        return Optional.empty();
    }

    @Test
    public void testPropertyIndexQuery() {
        // Create indexes on name and age
        config.setProperty("aerospike.graph.index.vertex.properties", "name,age");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            Vertex a = g.addV("person").
                    property("name", "Lyndon").
                    property("age", 29).
                    property("birthplace", "Canada").
                    next();

            final Optional<FireflyIndexMetadata.IndexInfo> nameIndex = getPropertyIndexInfo(fireflyGraph, "name", "Lyndon");
            assertTrue(nameIndex.isPresent());

            final Optional<FireflyIndexMetadata.IndexInfo> ageIndex = getPropertyIndexInfo(fireflyGraph, "age", 29L);
            assertTrue(ageIndex.isPresent());

            final Optional<FireflyIndexMetadata.IndexInfo> birthplaceIndex = getPropertyIndexInfo(fireflyGraph, "birthplace", "Canada");
            assertFalse(birthplaceIndex.isPresent());

            final Iterator<Vertex> vertexIteratorNameString = GraphQuery.create(graph).queryVertexSIndex(nameIndex.get(), P.eq("Lyndon"), fireflyGraph::vertexFromRecord, evaluationTimeout);
            Assert.assertTrue(vertexIteratorNameString.hasNext());
            Assert.assertEquals("Lyndon", vertexIteratorNameString.next().value("name"));
            Assert.assertFalse(vertexIteratorNameString.hasNext());

            final Iterator<Vertex> vertexIteratorNameInteger = GraphQuery.create(graph).queryVertexSIndex(nameIndex.get(), P.eq(1), fireflyGraph::vertexFromRecord, evaluationTimeout);
            final Iterator<Vertex> vertexIteratorNameLong = GraphQuery.create(graph).queryVertexSIndex(nameIndex.get(), P.eq(1L), fireflyGraph::vertexFromRecord, evaluationTimeout);
            Assert.assertFalse(vertexIteratorNameInteger.hasNext());
            Assert.assertFalse(vertexIteratorNameLong.hasNext());

            final Iterator<Vertex> vertexIteratorAgeString = GraphQuery.create(graph).queryVertexSIndex(ageIndex.get(), P.eq("29"), fireflyGraph::vertexFromRecord, evaluationTimeout);
            Assert.assertFalse(vertexIteratorAgeString.hasNext());

            final Iterator<Vertex> vertexIteratorAgeLong = GraphQuery.create(graph).queryVertexSIndex(ageIndex.get(), P.eq(29L), fireflyGraph::vertexFromRecord, evaluationTimeout);
            Assert.assertTrue(vertexIteratorAgeLong.hasNext());
            Assert.assertEquals(Integer.valueOf(29), vertexIteratorAgeLong.next().value("age"));

            final Iterator<Vertex> vertexIteratorAgeInteger = GraphQuery.create(graph).queryVertexSIndex(ageIndex.get(), P.eq(29), fireflyGraph::vertexFromRecord, evaluationTimeout);
            Assert.assertTrue(vertexIteratorAgeInteger.hasNext());
            Assert.assertEquals(Integer.valueOf(29), vertexIteratorAgeInteger.next().value("age"));
        }
    }

    @Test
    public void testPropertyScanQuery() {
        // No indexes for this test.
        config.setProperty("aerospike.graph.index.vertex.properties", "");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.addV("person").
                    property("name", "Lyndon").
                    property("age", 29).
                    property("birthplace", "Canada").
                    next();

            final Optional<FireflyIndexMetadata.IndexInfo> nameIndex = getPropertyIndexInfo(fireflyGraph, "name", "Lyndon");
            assertFalse(nameIndex.isPresent());

            final Optional<FireflyIndexMetadata.IndexInfo> ageIndex = getPropertyIndexInfo(fireflyGraph, "age", 29L);
            assertFalse(ageIndex.isPresent());

            final Optional<FireflyIndexMetadata.IndexInfo> birthplaceIndex = getPropertyIndexInfo(fireflyGraph, "birthplace", "Canada");
            assertFalse(birthplaceIndex.isPresent());

            final String setName = db.VERTEX_AERO_SET;
            final String binName = db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN;

            final Iterator<Vertex> vertexIteratorNameString = GraphQuery.create(graph).scanSet("name", setName, binName, P.eq("Lyndon"), fireflyGraph::vertexFromRecord, evaluationTimeout);
            Assert.assertTrue(vertexIteratorNameString.hasNext());
            Assert.assertEquals("Lyndon", vertexIteratorNameString.next().value("name"));
            Assert.assertFalse(vertexIteratorNameString.hasNext());

            final Iterator<Vertex> vertexIteratorNameInteger = GraphQuery.create(graph).scanSet("age", setName, binName, P.eq(1), fireflyGraph::vertexFromRecord, evaluationTimeout);
            final Iterator<Vertex> vertexIteratorNameLong = GraphQuery.create(graph).scanSet("age", setName, binName, P.eq(1L), fireflyGraph::vertexFromRecord, evaluationTimeout);
            Assert.assertFalse(vertexIteratorNameInteger.hasNext());
            Assert.assertFalse(vertexIteratorNameLong.hasNext());

            final Iterator<Vertex> vertexIteratorAgeString = GraphQuery.create(graph).scanSet("age", setName, binName, P.eq("29"), fireflyGraph::vertexFromRecord, evaluationTimeout);
            Assert.assertFalse(vertexIteratorAgeString.hasNext());

            final Iterator<Vertex> vertexIteratorAgeLong = GraphQuery.create(graph).scanSet("age", setName, binName, P.eq(29L), fireflyGraph::vertexFromRecord, evaluationTimeout);
            Assert.assertTrue(vertexIteratorAgeLong.hasNext());
            Assert.assertEquals(Integer.valueOf(29), vertexIteratorAgeLong.next().value("age"));

            final Iterator<Vertex> vertexIteratorAgeInteger = GraphQuery.create(graph).scanSet("age", setName, binName, P.eq(29), fireflyGraph::vertexFromRecord, evaluationTimeout);
            Assert.assertTrue(vertexIteratorAgeInteger.hasNext());
            Assert.assertEquals(Integer.valueOf(29), vertexIteratorAgeInteger.next().value("age"));

            final Iterator<Vertex> vertexIteratorBirthplaceString = GraphQuery.create(graph).scanSet("birthplace", setName, binName, P.eq("Canada"), fireflyGraph::vertexFromRecord, evaluationTimeout);
            Assert.assertTrue(vertexIteratorBirthplaceString.hasNext());
            Assert.assertEquals("Canada", vertexIteratorBirthplaceString.next().value("birthplace"));
            Assert.assertFalse(vertexIteratorBirthplaceString.hasNext());
        }
    }

    @Test
    public void testVertexLabelIndexEnabled() {
        config.setProperty(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), true);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.addV("person").
                    property("name", "Lyndon").
                    property("age", 29).
                    property("birthplace", "Canada").
                    next();

            final List<Map.Entry<String, String>> indices = AerospikeConnection.InfoOps.listExistingIndexes(
                    fireflyGraph.getBaseGraph());
            Map.Entry<String, String> vertexLabelIndex = null;
            for (final Map.Entry<String, String> index : indices) {
                if (index.getKey().equals(fireflyGraph.getBaseGraph().V_LABEL_INDEX_NAME)) {
                    vertexLabelIndex = index;
                }
            }
            Assert.assertNotNull(vertexLabelIndex);
            Assert.assertEquals(vertexLabelIndex.getValue(), (fireflyGraph.getBaseGraph().VERTEX_AERO_SET));

            final Iterator<FireflyVertex> vertices = GraphQuery.create(graph).scanSet(null, db.VERTEX_AERO_SET,db.LABEL_BIN, P.eq("person"), graph::vertexFromRecord, evaluationTimeout);
            Assert.assertTrue(vertices.hasNext());
        } finally {
            config.clearProperty(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG.toLowerCase());
        }
    }

    @Test
    public void testVertexLabelIndexDisabled() {
        config.setProperty(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), false);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.addV("person").
                    property("name", "Lyndon").
                    property("age", 29).
                    property("birthplace", "Canada").
                    next();

            final List<Map.Entry<String, String>> indices = AerospikeConnection.InfoOps.listExistingIndexes(
                    fireflyGraph.getBaseGraph());
            for (final Map.Entry<String, String> index : indices) {
                if (index.getKey().equals(fireflyGraph.getBaseGraph().V_LABEL_INDEX_NAME)) {
                    Assert.fail("Vertex label index found when it should have been disabled.");
                }
            }

            final Optional<FireflyIndexMetadata.IndexInfo> indexInfo = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "~label", "person");
            Assert.assertFalse(indexInfo.isPresent());
            final Iterator<FireflyVertex> vertices = GraphQuery.create(graph).scanSet(null, db.VERTEX_AERO_SET,db.LABEL_BIN, P.eq("person"), graph::vertexFromRecord, evaluationTimeout);
            Assert.assertTrue(vertices.hasNext());
        } finally {
            config.clearProperty(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG.toLowerCase());
        }
    }

    @Test
    public void testVertexLabelIndexUnion() {
        config.setProperty(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), true);
        config.setProperty(ConfigurationHelper.Keys.E_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), false);
        try (final FireflyGraph fireflyGraph1 = FireflyGraph.open(config)) {
            config.setProperty(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), false);
            try (final FireflyGraph fireflyGraph2 = FireflyGraph.open(config)) {
                final Optional<FireflyIndexMetadata.IndexInfo> vertexIndexInfo1 = fireflyGraph1.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "~label", "owns");
                Assert.assertTrue(vertexIndexInfo1.isPresent());
                final Optional<FireflyIndexMetadata.IndexInfo> vertexIndexInfo2 = fireflyGraph2.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "~label", "owns");
                Assert.assertTrue(vertexIndexInfo2.isPresent());
                final Optional<FireflyIndexMetadata.IndexInfo> edgeIndexInfo1 = fireflyGraph1.fireflyIndexMetadata.getPropertyIndexInfo(FireflyEdge.class, "~label", "owns");
                Assert.assertFalse(edgeIndexInfo1.isPresent());
                final Optional<FireflyIndexMetadata.IndexInfo> edgeIndexInfo2 = fireflyGraph2.fireflyIndexMetadata.getPropertyIndexInfo(FireflyEdge.class, "~label", "owns");
                Assert.assertFalse(edgeIndexInfo2.isPresent());
            }
        }
    }

    @Test
    public void testVertexLabelIndexPersists() {
        config.setProperty(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), true);
        config.setProperty(ConfigurationHelper.Keys.E_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), false);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
        }
        config.setProperty(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), false);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final Optional<FireflyIndexMetadata.IndexInfo> vertexIndexInfo = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "~label", "owns");
            Assert.assertTrue(vertexIndexInfo.isPresent());
            final Optional<FireflyIndexMetadata.IndexInfo> edgeIndexInfo = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyEdge.class, "~label", "owns");
            Assert.assertFalse(edgeIndexInfo.isPresent());
        }
    }
}
