package com.aerospike.firefly.io;

import com.aerospike.client.query.IndexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.aerospike.client.query.IndexType.NUMERIC;
import static com.aerospike.client.query.IndexType.STRING;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyIndexMetadata implements FireflyMetadata {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyIndexMetadata.class);
    private final List<IndexInfo> indexInfos = new ArrayList<>();
    private final AerospikeConnection db;

    /**
     * Default constructor for FireflyIndexMetadata.
     *
     * @param db The AerospikeConnection to use.
     */
    public FireflyIndexMetadata(final AerospikeConnection db) {
        this.db = db;
    }

    /**
     * Update the index metadata. Should be called periodically by timer and can be forcibly called by FireflyGraph.
     */
    @Override
    public void updateMetadata() {
        // Read the index metadata.
        final List<String> vertexPropertyIndexes =
                AerospikeConnection.InfoOps.listExistingIndexes(db.getClient(), db.getNamespace()).stream()
                        .map(Map.Entry::getKey).filter(s ->
                                s.startsWith(db.getVpIndexPrefix()) || db.V_LABEL_INDEX.equals(s) || db.E_LABEL_INDEX.equals(s)).
                        collect(Collectors.toList());

        // Update the index metadata.
        synchronized (FireflyIndexMetadata.class) {
            indexInfos.clear();
            for (final String indexName : vertexPropertyIndexes) {
                // If it is the vertex or edge label index, insert it.
                if (db.V_LABEL_INDEX.equals(indexName)) {
                    indexInfos.add(new IndexInfo(db.V_LABEL_INDEX, AerospikeConnection.LABEL, STRING, db.VERTEX_AERO_SET));
                    continue;
                } else if (db.E_LABEL_INDEX.equals(indexName)) {
                    indexInfos.add(new IndexInfo(db.E_LABEL_INDEX, AerospikeConnection.LABEL, STRING, db.EDGE_AERO_SET));
                    continue;
                }

                // Otherwise check if it is a property index.
                String propertyName = indexName.substring((db.getVpIndexPrefix() + "_").length());
                if (propertyName.endsWith("_" + IndexType.NUMERIC)) {
                    propertyName = propertyName.substring(0, propertyName.length() - IndexType.NUMERIC.toString().length() - 1);
                    indexInfos.add(new IndexInfo(indexName, propertyName, IndexType.NUMERIC, db.VERTEX_AERO_SET));
                } else if (propertyName.endsWith("_" + IndexType.STRING)) {
                    propertyName = propertyName.substring(0, propertyName.length() - IndexType.STRING.toString().length() - 1);
                    indexInfos.add(new IndexInfo(indexName, propertyName, IndexType.STRING, db.VERTEX_AERO_SET));
                } else {
                    LOG.warn("Unknown index type for index: {}.", indexName);
                }
            }
        }
    }

    /**
     * Get the index metadata.
     *
     * @return A copy of the index metadata.
     */
    public List<IndexInfo> getPropertyIndexInfos() {
        // Return copy.
        synchronized (FireflyIndexMetadata.class) {
            return new ArrayList<>(indexInfos);
        }
    }

    /**
     * This function finds the PropertyIndexInfo if it exists for the given key and value.
     *
     * @param key   Key of the property.
     * @param value Value of the property.
     * @return PropertyIndexInfo if it exists, empty optional otherwise.
     */
    public Optional<IndexInfo> getPropertyIndexInfo(final String key, final Object value) {
        final List<IndexInfo> indexInfosList = getPropertyIndexInfos();
        for (final IndexInfo indexInfo : indexInfosList) {
            if (indexInfo.key.equals(key) || (AerospikeConnection.LABEL.equals(indexInfo.key) && "~label".equals(key))) {
                if (Number.class.isAssignableFrom(value.getClass()) && indexInfo.indexType.equals(NUMERIC)) {
                    // If value is number, index type must also be numeric.
                    return Optional.of(indexInfo);
                } else if (String.class.isAssignableFrom(value.getClass()) && indexInfo.indexType.equals(STRING)) {
                    // If value is string, index type must also be string.
                    return Optional.of(indexInfo);
                }
            }
        }
        return Optional.empty();
    }


    /**
     * Class to hold all relevant information about indexes.
     */
    public static class IndexInfo {
        public final String indexName;
        public final String key;
        public final IndexType indexType;
        public final String setName;


        /**
         * Default constructor, simply populates the info class.
         *
         * @param indexName Name of the index.
         * @param key Key of the property.
         * @param indexType Type of the index.
         * @param setName Name of the set.
         */
        public IndexInfo(final String indexName,
                         final String key,
                         final IndexType indexType,
                         final String setName) {
            this.indexName = indexName;
            this.key = key;
            this.indexType = indexType;
            this.setName = setName;
        }

        @Override
        public String toString() {
            return "PropertyIndexInfo{" +
                    "indexName='" + indexName + '\'' +
                    ", propertyKey='" + key + '\'' +
                    ", indexType=" + indexType +
                    ", setName='" + setName + '\'' +
                    '}';
        }
    }
}
