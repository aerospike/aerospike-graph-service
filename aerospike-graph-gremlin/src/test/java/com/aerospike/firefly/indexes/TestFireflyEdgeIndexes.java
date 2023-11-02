package com.aerospike.firefly.indexes;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Ignore
public class TestFireflyEdgeIndexes extends TestFireflyIndexes {
    @Override
    protected void setProperty(final String propertyList) {
        config.setProperty("aerospike.graph.index.edge.properties", propertyList);
    }

    @Override
    protected String getIndexPrefix() {
        return db.getEpIndexPrefix();
    }

    @Override
    protected Optional<FireflyIndexMetadata.IndexInfo> getPropertyIndexInfo(final FireflyGraph fireflyGraph, final String key, final Object value) {
        return fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyEdge.class, key, value);
    }

    @Test
    public void testPropertyIndexQuery() {
        // Create indexes on name and age
        setProperty("from,years");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            Vertex lyndon = g.addV("person").
                    property("name", "Lyndon").
                    property("age", 29).
                    next();
            Vertex simon = g.addV("person").
                    property("name", "Simon").
                    property("age", 14).
                    next();
            g.addE("knows").
                    from(lyndon).
                    to(simon).
                    property("from", "BitQuill").
                    property("years", 3).
                    property("location", "Canada").
                    next();

            final Optional<FireflyIndexMetadata.IndexInfo> fromIndex = getPropertyIndexInfo(fireflyGraph, "from", "BitQuill");
            assertTrue(fromIndex.isPresent());

            final Optional<FireflyIndexMetadata.IndexInfo> yearsIndex = getPropertyIndexInfo(fireflyGraph, "years", 3);
            assertTrue(yearsIndex.isPresent());

            final Optional<FireflyIndexMetadata.IndexInfo> locationIndex = getPropertyIndexInfo(fireflyGraph, "location", "Canada");
            assertFalse(locationIndex.isPresent());

//            final Iterator<Edge> edgeIteratorNameString = fireflyGraph.queryIndex(fromIndex.get(), P.eq("BitQuill"), fireflyGraph::edgeFromRecord);
//            Assert.assertTrue(edgeIteratorNameString.hasNext());
//            Assert.assertEquals("BitQuill", edgeIteratorNameString.next().value("from"));
//            Assert.assertFalse(edgeIteratorNameString.hasNext());
//
//            final Iterator<Edge> edgeIteratorNameInteger = fireflyGraph.queryIndex(fromIndex.get(), P.eq(1), fireflyGraph::edgeFromRecord);
//            final Iterator<Edge> edgeIteratorNameLong = fireflyGraph.queryIndex(fromIndex.get(), P.eq(1L), fireflyGraph::edgeFromRecord);
//            Assert.assertFalse(edgeIteratorNameInteger.hasNext());
//            Assert.assertFalse(edgeIteratorNameLong.hasNext());
//
//            final Iterator<Edge> edgeIteratorAgeString = fireflyGraph.queryIndex(yearsIndex.get(), P.eq("29"), fireflyGraph::edgeFromRecord);
//            Assert.assertFalse(edgeIteratorAgeString.hasNext());
//
//            // Looks like a mistake here.
//            final Iterator<Edge> edgeIteratorAgeLong = fireflyGraph.queryIndex(yearsIndex.get(), P.eq(3L), fireflyGraph::edgeFromRecord);
//            Assert.assertTrue(edgeIteratorAgeLong.hasNext());
//            Assert.assertEquals(Integer.valueOf(3), edgeIteratorAgeLong.next().value("years"));
//
//            final Iterator<Edge> edgeIteratorAgeInteger = fireflyGraph.queryIndex(yearsIndex.get(), P.eq(3), fireflyGraph::edgeFromRecord);
//            Assert.assertTrue(edgeIteratorAgeInteger.hasNext());
//            Assert.assertEquals(Integer.valueOf(3), edgeIteratorAgeInteger.next().value("years"));
        }
    }

    @Test
    public void testPropertyScanQuery() {
        // No indexes for this test.
        setProperty("");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            Vertex lyndon = g.addV("person").
                    property("name", "Lyndon").
                    property("age", 29).
                    next();
            Vertex simon = g.addV("person").
                    property("name", "Simon").
                    property("age", 14).
                    next();
            g.addE("knows").
                    from(lyndon).
                    to(simon).
                    property("from", "BitQuill").
                    property("years", 3).
                    property("location", "Canada").
                    next();

            final Optional<FireflyIndexMetadata.IndexInfo> fromIndex = getPropertyIndexInfo(fireflyGraph, "from", "BitQuill");
            assertFalse(fromIndex.isPresent());

            final Optional<FireflyIndexMetadata.IndexInfo> yearIndex = getPropertyIndexInfo(fireflyGraph, "years", 3);
            assertFalse(yearIndex.isPresent());

            final Optional<FireflyIndexMetadata.IndexInfo> locationIndex = getPropertyIndexInfo(fireflyGraph, "location", "Canada");
            assertFalse(locationIndex.isPresent());

            final String setfrom = db.EDGE_AERO_SET;
            final String binfrom = db.PROPERTIES_BIN;

//            final Iterator<Edge> edgeIteratorFromString = fireflyGraph.queryScan("from", setfrom, binfrom, P.eq("BitQuill"), fireflyGraph::edgeFromRecord);
//            Assert.assertTrue(edgeIteratorFromString.hasNext());
//            Assert.assertEquals("BitQuill", edgeIteratorFromString.next().value("from"));
//            Assert.assertFalse(edgeIteratorFromString.hasNext());
//
//            final Iterator<Edge> edgeIteratorFromInteger = fireflyGraph.queryScan("years", setfrom, binfrom, P.eq(1), fireflyGraph::edgeFromRecord);
//            final Iterator<Edge> edgeIteratorFromLong = fireflyGraph.queryScan("years", setfrom, binfrom, P.eq(1L), fireflyGraph::edgeFromRecord);
//            Assert.assertFalse(edgeIteratorFromInteger.hasNext());
//            Assert.assertFalse(edgeIteratorFromLong.hasNext());
//
//            final Iterator<Edge> edgeIteratorYearString = fireflyGraph.queryScan("years", setfrom, binfrom, P.eq("29"), fireflyGraph::edgeFromRecord);
//            Assert.assertFalse(edgeIteratorYearString.hasNext());
//
//            final Iterator<Edge> edgeIteratorYearLong = fireflyGraph.queryScan("years", setfrom, binfrom, P.eq(3L), fireflyGraph::edgeFromRecord);
//            Assert.assertTrue(edgeIteratorYearLong.hasNext());
//            Assert.assertEquals(Integer.valueOf(3), edgeIteratorYearLong.next().value("years"));
//
//            final Iterator<Edge> edgeIteratorYearInteger = fireflyGraph.queryScan("years", setfrom, binfrom, P.eq(3), fireflyGraph::edgeFromRecord);
//            Assert.assertTrue(edgeIteratorYearInteger.hasNext());
//            Assert.assertEquals(Integer.valueOf(3), edgeIteratorYearInteger.next().value("years"));
//
//            final Iterator<Edge> edgeIteratorLocationString = fireflyGraph.queryScan("location", setfrom, binfrom, P.eq("Canada"), fireflyGraph::edgeFromRecord);
//            Assert.assertTrue(edgeIteratorLocationString.hasNext());
//            Assert.assertEquals("Canada", edgeIteratorLocationString.next().value("location"));
//            Assert.assertFalse(edgeIteratorLocationString.hasNext());
        }
    }

    @Test
    public void testEdgeLabelIndexEnabled() {
        config.setProperty(ConfigurationHelper.Keys.E_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), true);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            final Vertex person = g.addV("person").next();
            final Vertex cat = g.addV("cat").next();
            g.addE("owns").from(person).to(cat).iterate();

            final List<Map.Entry<String, String>> indices = AerospikeConnection.InfoOps.listExistingIndexes(
                    fireflyGraph.getBaseGraph().getClient(), fireflyGraph.getBaseGraph().getNamespace());
            Map.Entry<String, String> edgeLabelIndex = null;
            for (final Map.Entry<String, String> index : indices) {
                if (index.getKey().equals(fireflyGraph.getBaseGraph().E_LABEL_INDEX_NAME)) {
                    edgeLabelIndex = index;
                }
            }
            Assert.assertNotNull(edgeLabelIndex);
            Assert.assertEquals(edgeLabelIndex.getValue(), (fireflyGraph.getBaseGraph().EDGE_AERO_SET));

//            final Iterator<FireflyEdge> edges = fireflyGraph.queryScan(null, db.EDGE_AERO_SET, AerospikeConnection.LABEL, P.eq("owns"), graph::edgeFromRecord);
//            Assert.assertTrue(edges.hasNext());
        } finally {
            config.clearProperty(ConfigurationHelper.Keys.E_LABEL_INDEX_ENABLED_FLAG.toLowerCase());
        }
    }

    @Test
    public void testEdgeLabelIndexDisabled() {
        config.setProperty(ConfigurationHelper.Keys.E_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), false);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {

            final GraphTraversalSource g = fireflyGraph.traversal();
            final Vertex person = g.addV("person").next();
            final Vertex cat = g.addV("cat").next();
            g.addE("owns").from(person).to(cat).iterate();

            final List<Map.Entry<String, String>> indices = AerospikeConnection.InfoOps.listExistingIndexes(
                    fireflyGraph.getBaseGraph().getClient(), fireflyGraph.getBaseGraph().getNamespace());
            for (final Map.Entry<String, String> index : indices) {
                if (index.getKey().equals(fireflyGraph.getBaseGraph().E_LABEL_INDEX_NAME)) {
                    Assert.fail("Vertex label index found when it should have been disabled.");
                }
            }

            final Optional<FireflyIndexMetadata.IndexInfo> indexInfo = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyEdge.class, "~label", "owns");
            Assert.assertTrue(indexInfo.isEmpty());
//            final Iterator<FireflyEdge> edges = fireflyGraph.queryScan(null, db.EDGE_AERO_SET, AerospikeConnection.LABEL, P.eq("owns"), graph::edgeFromRecord);
//            Assert.assertTrue(edges.hasNext());
        } finally {
            config.clearProperty(ConfigurationHelper.Keys.E_LABEL_INDEX_ENABLED_FLAG.toLowerCase());
        }
    }

    @Test
    public void testEdgeLabelIndexUnion() {
        config.setProperty(ConfigurationHelper.Keys.E_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), true);
        config.setProperty(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), false);
        try (final FireflyGraph fireflyGraph1 = FireflyGraph.open(config)) {
            config.setProperty(ConfigurationHelper.Keys.E_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), false);
            try (final FireflyGraph fireflyGraph2 = FireflyGraph.open(config)) {
                final Optional<FireflyIndexMetadata.IndexInfo> vertexIndexInfo1 = fireflyGraph1.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "~label", "owns");
                Assert.assertFalse(vertexIndexInfo1.isPresent());
                final Optional<FireflyIndexMetadata.IndexInfo> vertexIndexInfo2 = fireflyGraph2.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "~label", "owns");
                Assert.assertFalse(vertexIndexInfo2.isPresent());
                final Optional<FireflyIndexMetadata.IndexInfo> edgeIndexInfo1 = fireflyGraph1.fireflyIndexMetadata.getPropertyIndexInfo(FireflyEdge.class, "~label", "owns");
                Assert.assertTrue(edgeIndexInfo1.isPresent());
                final Optional<FireflyIndexMetadata.IndexInfo> edgeIndexInfo2 = fireflyGraph2.fireflyIndexMetadata.getPropertyIndexInfo(FireflyEdge.class, "~label", "owns");
                Assert.assertTrue(edgeIndexInfo2.isPresent());
            }
        }
    }

    @Test
    public void testEdgeLabelIndexPersists() {
        config.setProperty(ConfigurationHelper.Keys.E_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), true);
        config.setProperty(ConfigurationHelper.Keys.V_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), false);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
        }
        config.setProperty(ConfigurationHelper.Keys.E_LABEL_INDEX_ENABLED_FLAG.toLowerCase(), false);
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final Optional<FireflyIndexMetadata.IndexInfo> vertexIndexInfo = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, "~label", "owns");
            Assert.assertFalse(vertexIndexInfo.isPresent());
            final Optional<FireflyIndexMetadata.IndexInfo> edgeIndexInfo = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyEdge.class, "~label", "owns");
            Assert.assertTrue(edgeIndexInfo.isPresent());
        }
    }
}
