package com.aerospike.firefly.indexes;

import com.aerospike.client.query.IndexType;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.ConfigurationHelper;
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
public abstract class TestFireflyIndexes extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return false;
    }

    @Before
    public void clearIndexes() {
        // Delete all indexes.
        graph.getBaseGraph().clearNamespace();
    }

    protected abstract void setProperty(final String propertyList);

    protected abstract String getIndexPrefix();

    protected abstract Optional<FireflyIndexMetadata.IndexInfo> getPropertyIndexInfo(final FireflyGraph fireflyGraph, final String key, final Object value);

    @Test
    public void testCreatedPropertyIndexesExist() {
        // Create indexes on name and age.
        setProperty("name,age");

        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final List<String> existingIndexes =
                    AerospikeConnection.InfoOps.listExistingIndexes(db.getClient(), db.getNamespace()).stream()
                            .map(Map.Entry::getKey).collect(Collectors.toList());

            Assert.assertTrue(existingIndexes.contains(getIndexPrefix() + "_" + "name" + "_" + IndexType.NUMERIC));
            Assert.assertTrue(existingIndexes.contains(getIndexPrefix() + "_" + "name" + "_" + IndexType.STRING));
            Assert.assertTrue(existingIndexes.contains(getIndexPrefix() + "_" + "age" + "_" + IndexType.NUMERIC));
            Assert.assertTrue(existingIndexes.contains(getIndexPrefix() + "_" + "age" + "_" + IndexType.STRING));
            Assert.assertFalse(existingIndexes.contains(getIndexPrefix() + "_" + "birthplace" + "_" + IndexType.NUMERIC));
            Assert.assertFalse(existingIndexes.contains(getIndexPrefix() + "_" + "birthplace" + "_" + IndexType.STRING));

        }
    }

    @Test
    public void testOldIndexesExist() {
        // Create indexes on name and age.
        setProperty("name,age");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
        }

        setProperty("birthplace");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final List<String> existingIndexes =
                    AerospikeConnection.InfoOps.listExistingIndexes(db.getClient(), db.getNamespace()).stream()
                            .map(Map.Entry::getKey).collect(Collectors.toList());

            Assert.assertTrue(existingIndexes.contains(getIndexPrefix() + "_" + "name" + "_" + IndexType.NUMERIC));
            Assert.assertTrue(existingIndexes.contains(getIndexPrefix() + "_" + "name" + "_" + IndexType.STRING));
            Assert.assertTrue(existingIndexes.contains(getIndexPrefix() + "_" + "age" + "_" + IndexType.NUMERIC));
            Assert.assertTrue(existingIndexes.contains(getIndexPrefix() + "_" + "age" + "_" + IndexType.STRING));
            Assert.assertTrue(existingIndexes.contains(getIndexPrefix() + "_" + "birthplace" + "_" + IndexType.NUMERIC));
            Assert.assertTrue(existingIndexes.contains(getIndexPrefix() + "_" + "birthplace" + "_" + IndexType.STRING));
        }
    }

    @Test
    public void testGetPropertyIndexInfo() {
        // Create indexes on name and age
        setProperty("name,age");

        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final Optional<FireflyIndexMetadata.IndexInfo> nameIndexNumeric = getPropertyIndexInfo(fireflyGraph, "name", 1);
            final Optional<FireflyIndexMetadata.IndexInfo> nameIndexString = getPropertyIndexInfo(fireflyGraph, "name", "1");
            final Optional<FireflyIndexMetadata.IndexInfo> ageIndexNumeric = getPropertyIndexInfo(fireflyGraph, "age", 1);
            final Optional<FireflyIndexMetadata.IndexInfo> ageIndexString = getPropertyIndexInfo(fireflyGraph, "age", "1");
            final Optional<FireflyIndexMetadata.IndexInfo> birthplaceIndexNumeric = getPropertyIndexInfo(fireflyGraph, "birthplace", 1);
            final Optional<FireflyIndexMetadata.IndexInfo> birthplaceIndexString = getPropertyIndexInfo(fireflyGraph, "birthplace", "1");

            assertTrue(nameIndexNumeric.isPresent());
            assertTrue(nameIndexString.isPresent());
            assertTrue(ageIndexNumeric.isPresent());
            assertTrue(ageIndexString.isPresent());
            assertFalse(birthplaceIndexNumeric.isPresent());
            assertFalse(birthplaceIndexString.isPresent());
        }
    }

    @Test
    public void testGetVertexPropertyIndexInfoUnionsOldIndexInfo() {
        // Create indexes on name and age
        setProperty("name,age");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final Optional<FireflyIndexMetadata.IndexInfo> nameIndexNumeric = getPropertyIndexInfo(fireflyGraph, "name", 1);
            final Optional<FireflyIndexMetadata.IndexInfo> nameIndexString = getPropertyIndexInfo(fireflyGraph, "name", "1");
            final Optional<FireflyIndexMetadata.IndexInfo> ageIndexNumeric = getPropertyIndexInfo(fireflyGraph, "age", 1);
            final Optional<FireflyIndexMetadata.IndexInfo> ageIndexString = getPropertyIndexInfo(fireflyGraph, "age", "1");
            final Optional<FireflyIndexMetadata.IndexInfo> birthplaceIndexNumeric = getPropertyIndexInfo(fireflyGraph, "birthplace", 1);
            final Optional<FireflyIndexMetadata.IndexInfo> birthplaceIndexString = getPropertyIndexInfo(fireflyGraph, "birthplace", "1");
            assertTrue(nameIndexNumeric.isPresent());
            assertTrue(nameIndexString.isPresent());
            assertTrue(ageIndexNumeric.isPresent());
            assertTrue(ageIndexString.isPresent());
            assertFalse(birthplaceIndexNumeric.isPresent());
            assertFalse(birthplaceIndexString.isPresent());
        }

        // Create index on only birthplace (expect name and age to be removed)
        setProperty("birthplace");
        try (final FireflyGraph fireflyGraph = FireflyGraph.open(config)) {
            final Optional<FireflyIndexMetadata.IndexInfo> nameIndexNumeric = getPropertyIndexInfo(fireflyGraph, "name", 1);
            final Optional<FireflyIndexMetadata.IndexInfo> nameIndexString = getPropertyIndexInfo(fireflyGraph, "name", "1");
            final Optional<FireflyIndexMetadata.IndexInfo> ageIndexNumeric = getPropertyIndexInfo(fireflyGraph, "age", 1);
            final Optional<FireflyIndexMetadata.IndexInfo> ageIndexString = getPropertyIndexInfo(fireflyGraph, "age", "1");
            final Optional<FireflyIndexMetadata.IndexInfo> birthplaceIndexNumeric = getPropertyIndexInfo(fireflyGraph, "birthplace", 1);
            final Optional<FireflyIndexMetadata.IndexInfo> birthplaceIndexString = getPropertyIndexInfo(fireflyGraph, "birthplace", "1");

            assertTrue(nameIndexNumeric.isPresent());
            assertTrue(nameIndexString.isPresent());
            assertTrue(ageIndexNumeric.isPresent());
            assertTrue(ageIndexString.isPresent());
            assertTrue(birthplaceIndexNumeric.isPresent());
            assertTrue(birthplaceIndexString.isPresent());
        }
    }
}
