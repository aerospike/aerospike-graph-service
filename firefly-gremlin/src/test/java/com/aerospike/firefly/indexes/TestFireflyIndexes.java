package com.aerospike.firefly.indexes;

import com.aerospike.client.query.IndexType;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class TestFireflyIndexes extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return false;
    }

    @Before
    public void clearIndexes() {
        // Delete all indexes.
        graph.getBaseGraph().clearNamespace();
    }

    @Test
    public void testCreatedIndexesExist() {
        // Create indexes on name and age.
        config.setProperty("vertex_property_indexes", "name,age");

        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final List<String> existingIndexes =
                    AerospikeConnection.InfoOps.listExistingIndexes(db.getClient(), db.getNamespace()).stream()
                            .map(Map.Entry::getKey).collect(Collectors.toList());

            Assert.assertTrue(existingIndexes.contains(db.getVpIndexPrefix() + "_" + "name" + "_" + IndexType.NUMERIC));
            Assert.assertTrue(existingIndexes.contains(db.getVpIndexPrefix() + "_" + "name" + "_" + IndexType.STRING));
            Assert.assertTrue(existingIndexes.contains(db.getVpIndexPrefix() + "_" + "age" + "_" + IndexType.NUMERIC));
            Assert.assertTrue(existingIndexes.contains(db.getVpIndexPrefix() + "_" + "age" + "_" + IndexType.STRING));
            Assert.assertFalse(existingIndexes.contains(db.getVpIndexPrefix() + "_" + "birthplace" + "_" + IndexType.NUMERIC));
            Assert.assertFalse(existingIndexes.contains(db.getVpIndexPrefix() + "_" + "birthplace" + "_" + IndexType.STRING));

        }
    }

    @Test
    public void testOldIndexesExist() {
        // Create indexes on name and age.
        config.setProperty("vertex_property_indexes", "name,age");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
        }

        config.setProperty("vertex_property_indexes", "birthplace");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final List<String> existingIndexes =
                    AerospikeConnection.InfoOps.listExistingIndexes(db.getClient(), db.getNamespace()).stream()
                            .map(Map.Entry::getKey).collect(Collectors.toList());

            Assert.assertTrue(existingIndexes.contains(db.getVpIndexPrefix() + "_" + "name" + "_" + IndexType.NUMERIC));
            Assert.assertTrue(existingIndexes.contains(db.getVpIndexPrefix() + "_" + "name" + "_" + IndexType.STRING));
            Assert.assertTrue(existingIndexes.contains(db.getVpIndexPrefix() + "_" + "age" + "_" + IndexType.NUMERIC));
            Assert.assertTrue(existingIndexes.contains(db.getVpIndexPrefix() + "_" + "age" + "_" + IndexType.STRING));
            Assert.assertTrue(existingIndexes.contains(db.getVpIndexPrefix() + "_" + "birthplace" + "_" + IndexType.NUMERIC));
            Assert.assertTrue(existingIndexes.contains(db.getVpIndexPrefix() + "_" + "birthplace" + "_" + IndexType.STRING));
        }
    }

    @Test
    public void testGetPropertyIndexInfo() {
        // Create indexes on name and age
        config.setProperty("vertex_property_indexes", "name,age");

        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final Optional<FireflyIndexMetadata.IndexInfo> nameIndexNumeric = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("name", 1);
            final Optional<FireflyIndexMetadata.IndexInfo> nameIndexString = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("name", "1");
            final Optional<FireflyIndexMetadata.IndexInfo> ageIndexNumeric = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("age", 1);
            final Optional<FireflyIndexMetadata.IndexInfo> ageIndexString = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("age", "1");
            final Optional<FireflyIndexMetadata.IndexInfo> birthplaceIndexNumeric = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("birthplace", 1);
            final Optional<FireflyIndexMetadata.IndexInfo> birthplaceIndexString =fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("birthplace", "1");

            assertTrue(nameIndexNumeric.isPresent());
            assertTrue(nameIndexString.isPresent());
            assertTrue(ageIndexNumeric.isPresent());
            assertTrue(ageIndexString.isPresent());
            assertFalse(birthplaceIndexNumeric.isPresent());
            assertFalse(birthplaceIndexString.isPresent());
        }
    }

    @Test
    public void testGetPropertyIndexInfoUnionsOldIndexInfo() {
        // Create indexes on name and age
        config.setProperty("vertex_property_indexes", "name,age");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final Optional<FireflyIndexMetadata.IndexInfo> nameIndexNumeric = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("name", 1);
            final Optional<FireflyIndexMetadata.IndexInfo> nameIndexString = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("name", "1");
            final Optional<FireflyIndexMetadata.IndexInfo> ageIndexNumeric = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("age", 1);
            final Optional<FireflyIndexMetadata.IndexInfo> ageIndexString = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("age", "1");
            final Optional<FireflyIndexMetadata.IndexInfo> birthplaceIndexNumeric = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("birthplace", 1);
            final Optional<FireflyIndexMetadata.IndexInfo> birthplaceIndexString = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("birthplace", "1");
            assertTrue(nameIndexNumeric.isPresent());
            assertTrue(nameIndexString.isPresent());
            assertTrue(ageIndexNumeric.isPresent());
            assertTrue(ageIndexString.isPresent());
            assertFalse(birthplaceIndexNumeric.isPresent());
            assertFalse(birthplaceIndexString.isPresent());
        }

        // Create index on only birthplace (expect name and age to be removed)
        config.setProperty("vertex_property_indexes", "birthplace");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final Optional<FireflyIndexMetadata.IndexInfo> nameIndexNumeric = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("name", 1);
            final Optional<FireflyIndexMetadata.IndexInfo> nameIndexString = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("name", "1");
            final Optional<FireflyIndexMetadata.IndexInfo> ageIndexNumeric = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("age", 1);
            final Optional<FireflyIndexMetadata.IndexInfo> ageIndexString = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("age", "1");
            final Optional<FireflyIndexMetadata.IndexInfo> birthplaceIndexNumeric = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("birthplace", 1);
            final Optional<FireflyIndexMetadata.IndexInfo> birthplaceIndexString =fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("birthplace", "1");

            assertTrue(nameIndexNumeric.isPresent());
            assertTrue(nameIndexString.isPresent());
            assertTrue(ageIndexNumeric.isPresent());
            assertTrue(ageIndexString.isPresent());
            assertTrue(birthplaceIndexNumeric.isPresent());
            assertTrue(birthplaceIndexString.isPresent());
        }
    }

    @Test
    public void testIndexQuery() {
        // Create indexes on name and age
        config.setProperty("vertex_property_indexes", "name,age");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.addV("person").
                    property("name", "Lyndon").
                    property("age", 29).
                    property("birthplace", "Canada").
                    next();

            final Optional<FireflyIndexMetadata.IndexInfo> nameIndex = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("name", "Lyndon");
            assertTrue(nameIndex.isPresent());

            final Optional<FireflyIndexMetadata.IndexInfo> ageIndex = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("age", 29L);
            assertTrue(ageIndex.isPresent());

            final Optional<FireflyIndexMetadata.IndexInfo> birthplaceIndex = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("birthplace", "Canada");
            assertFalse(birthplaceIndex.isPresent());

            final Iterator<Vertex> vertexIteratorNameString = fireflyGraph.queryIndex(nameIndex.get(), P.eq("Lyndon"), fireflyGraph::vertexFromRecord);
            Assert.assertTrue(vertexIteratorNameString.hasNext());
            Assert.assertEquals("Lyndon", vertexIteratorNameString.next().value("name"));
            Assert.assertFalse(vertexIteratorNameString.hasNext());

            final Iterator<Vertex> vertexIteratorNameInteger = fireflyGraph.queryIndex(nameIndex.get(), P.eq(1), fireflyGraph::vertexFromRecord);
            final Iterator<Vertex> vertexIteratorNameLong = fireflyGraph.queryIndex(nameIndex.get(), P.eq(1L), fireflyGraph::vertexFromRecord);
            Assert.assertFalse(vertexIteratorNameInteger.hasNext());
            Assert.assertFalse(vertexIteratorNameLong.hasNext());

            final Iterator<Vertex> vertexIteratorAgeString = fireflyGraph.queryIndex(ageIndex.get(), P.eq("29"), fireflyGraph::vertexFromRecord);
            Assert.assertFalse(vertexIteratorAgeString.hasNext());

            final Iterator<Vertex> vertexIteratorAgeLong = fireflyGraph.queryIndex(ageIndex.get(), P.eq(29L), fireflyGraph::vertexFromRecord);
            Assert.assertTrue(vertexIteratorAgeLong.hasNext());
            Assert.assertEquals(Integer.valueOf(29), vertexIteratorAgeLong.next().value("age"));

            final Iterator<Vertex> vertexIteratorAgeInteger = fireflyGraph.queryIndex(ageIndex.get(), P.eq(29), fireflyGraph::vertexFromRecord);
            Assert.assertTrue(vertexIteratorAgeInteger.hasNext());
            Assert.assertEquals(Integer.valueOf(29), vertexIteratorAgeInteger.next().value("age"));
        }
    }

    @Test
    public void testScanQuery() {
        // Create indexes on name and age
        config.setProperty("vertex_property_indexes", "");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = fireflyGraph.traversal();
            g.addV("person").
                    property("name", "Lyndon").
                    property("age", 29).
                    property("birthplace", "Canada").
                    next();

            final Optional<FireflyIndexMetadata.IndexInfo> nameIndex = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("name", "Lyndon");
            assertFalse(nameIndex.isPresent());

            final Optional<FireflyIndexMetadata.IndexInfo> ageIndex = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("age", 29L);
            assertFalse(ageIndex.isPresent());

            final Optional<FireflyIndexMetadata.IndexInfo> birthplaceIndex = fireflyGraph.fireflyIndexMetadata.getPropertyIndexInfo("birthplace", "Canada");
            assertFalse(birthplaceIndex.isPresent());

            final Iterator<Vertex> vertexIteratorNameString = fireflyGraph.queryScan("name", P.eq("Lyndon"), fireflyGraph::vertexFromRecord);
            Assert.assertTrue(vertexIteratorNameString.hasNext());
            Assert.assertEquals("Lyndon", vertexIteratorNameString.next().value("name"));
            Assert.assertFalse(vertexIteratorNameString.hasNext());

            final Iterator<Vertex> vertexIteratorNameInteger = fireflyGraph.queryScan("age", P.eq(1), fireflyGraph::vertexFromRecord);
            final Iterator<Vertex> vertexIteratorNameLong = fireflyGraph.queryScan("age", P.eq(1L), fireflyGraph::vertexFromRecord);
            Assert.assertFalse(vertexIteratorNameInteger.hasNext());
            Assert.assertFalse(vertexIteratorNameLong.hasNext());

            final Iterator<Vertex> vertexIteratorAgeString = fireflyGraph.queryScan("age", P.eq("29"), fireflyGraph::vertexFromRecord);
            Assert.assertFalse(vertexIteratorAgeString.hasNext());

            final Iterator<Vertex> vertexIteratorAgeLong = fireflyGraph.queryScan("age", P.eq(29L), fireflyGraph::vertexFromRecord);
            Assert.assertTrue(vertexIteratorAgeLong.hasNext());
            Assert.assertEquals(Integer.valueOf(29), vertexIteratorAgeLong.next().value("age"));

            final Iterator<Vertex> vertexIteratorAgeInteger = fireflyGraph.queryScan("age", P.eq(29), fireflyGraph::vertexFromRecord);
            Assert.assertTrue(vertexIteratorAgeInteger.hasNext());
            Assert.assertEquals(Integer.valueOf(29), vertexIteratorAgeInteger.next().value("age"));

            final Iterator<Vertex> vertexIteratorBirthplaceString = fireflyGraph.queryScan("birthplace", P.eq("Canada"), fireflyGraph::vertexFromRecord);
            Assert.assertTrue(vertexIteratorBirthplaceString.hasNext());
            Assert.assertEquals("Canada", vertexIteratorBirthplaceString.next().value("birthplace"));
            Assert.assertFalse(vertexIteratorBirthplaceString.hasNext());
        }
    }
}
