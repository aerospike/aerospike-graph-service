package com.aerospike.firefly.io;

import com.aerospike.client.query.IndexType;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyVertex;
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
        final List<String> indexes = AerospikeConnection.InfoOps.listUsableIndexes(db, db.getNamespace());

        // Update the index metadata.
        synchronized (FireflyIndexMetadata.class) {
            indexInfos.clear();
            for (final String indexName : indexes) {
                // If it is the vertex or edge label index, insert it.
                if (db.V_LABEL_INDEX_NAME.equals(indexName)) {
                    indexInfos.add(new IndexInfo(db.V_LABEL_INDEX_NAME,db.LABEL_BIN, STRING, db.VERTEX_AERO_SET));
                    continue;
                } else if (db.E_LABEL_INDEX_NAME.equals(indexName)) {
                    indexInfos.add(new IndexInfo(db.E_LABEL_INDEX_NAME,db.LABEL_BIN, STRING, db.EDGE_AERO_SET));
                    continue;
                }

                // Otherwise, if it is a property index, we need to strip the info.
                String propertyName;
                final String setName;
                if (indexName.startsWith(db.getVpIndexPrefix())) {
                    setName = db.VERTEX_AERO_SET;
                    propertyName = indexName.substring((db.getVpIndexPrefix() + "_").length());
                } else if (indexName.startsWith(db.getEpIndexPrefix())) {
                    setName = db.EDGE_AERO_SET;
                    propertyName = indexName.substring((db.getEpIndexPrefix() + "_").length());
                } else {
                    // Not a property index.
                    LOG.warn("Unknown index: {}.", indexName);
                    continue;
                }

                if (propertyName.endsWith("_" + IndexType.NUMERIC)) {
                    propertyName = propertyName.substring(0, propertyName.length() - IndexType.NUMERIC.toString().length() - 1);
                    indexInfos.add(new IndexInfo(indexName, propertyName, IndexType.NUMERIC, setName));
                } else if (propertyName.endsWith("_" + IndexType.STRING)) {
                    propertyName = propertyName.substring(0, propertyName.length() - IndexType.STRING.toString().length() - 1);
                    indexInfos.add(new IndexInfo(indexName, propertyName, IndexType.STRING, setName));
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
    public Optional<IndexInfo> getPropertyIndexInfo(final Class<? extends FireflyElement> elementClass, final String key, final Object value) {
        final List<IndexInfo> indexInfosList = getPropertyIndexInfos();
        for (final IndexInfo indexInfo : indexInfosList) {
            if (FireflyVertex.class.isAssignableFrom(elementClass)) {
                if (indexInfo.setName.equals(db.V_LABEL_INDEX_NAME) || indexInfo.setName.equals(db.VERTEX_AERO_SET)) {
                    if (indexInfo.key.equals(key) || (db.LABEL_BIN.equals(indexInfo.key) && "~label".equals(key))) {
                        if (Number.class.isAssignableFrom(value.getClass()) && indexInfo.indexType.equals(NUMERIC)) {
                            // If value is number, index type must also be numeric.
                            return Optional.of(indexInfo);
                        } else if (String.class.isAssignableFrom(value.getClass()) && indexInfo.indexType.equals(STRING)) {
                            // If value is string, index type must also be string.
                            return Optional.of(indexInfo);
                        }
                    }
                }
            } else if (FireflyEdge.class.isAssignableFrom(elementClass)) {
                if (indexInfo.setName.equals(db.E_LABEL_INDEX_NAME) || indexInfo.setName.equals(db.EDGE_AERO_SET)) {
                    if (indexInfo.key.equals(key) || (db.LABEL_BIN.equals(indexInfo.key) && "~label".equals(key))) {
                        if (Number.class.isAssignableFrom(value.getClass()) && indexInfo.indexType.equals(NUMERIC)) {
                            // If value is number, index type must also be numeric.
                            return Optional.of(indexInfo);
                        } else if (String.class.isAssignableFrom(value.getClass()) && indexInfo.indexType.equals(STRING)) {
                            // If value is string, index type must also be string.
                            return Optional.of(indexInfo);
                        }
                    }
                }
            } else {
                throw new IllegalArgumentException("Cannot get property index info for unknown element class: " + elementClass);
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
         * @param key       Key of the property.
         * @param indexType Type of the index.
         * @param setName   Name of the set.
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
