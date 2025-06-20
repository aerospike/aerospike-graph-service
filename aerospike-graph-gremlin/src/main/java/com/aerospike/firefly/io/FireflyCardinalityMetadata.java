package com.aerospike.firefly.io;

import com.aerospike.client.query.IndexType;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyCardinalityMetadata implements FireflyMetadata {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyCardinalityMetadata.class);
    private static final String ENTRIES = "entries=";
    private static final String ENTRIES_PER_BVAL = "entries_per_bval=";
    private static final String infoQueryFormat = "sindex/%s/%s"; // "sindex/<namespace>/<index name>
    private CardinalityInfo vertexLabelCardinalityInfo;
    private CardinalityInfo edgeLabelCardinalityInfo;
    private List<CardinalityInfo> vertexNumericPropertyCardinalityInfo = new ArrayList<>();
    private List<CardinalityInfo> vertexStringPropertyCardinalityInfo = new ArrayList<>();
    private List<CardinalityInfo> edgeNumericPropertyCardinalityInfo = new ArrayList<>();
    private List<CardinalityInfo> edgeStringPropertyCardinalityInfo = new ArrayList<>();
    private final AerospikeConnection db;
    private final String vertexLabelIndex;
    private final String edgeLabelIndex;
    private final FireflyIndexMetadata indexMetadata;

    private String previousFailure;

    public FireflyCardinalityMetadata(final AerospikeConnection db,
                                      final String vertexLabelIndex,
                                      final String edgeLabelIndex,
                                      final FireflyIndexMetadata indexMetadata) {
        this.db = db;
        this.indexMetadata = indexMetadata;
        this.vertexLabelIndex = String.format(infoQueryFormat, db.getNamespace(), vertexLabelIndex);
        this.edgeLabelIndex = String.format(infoQueryFormat, db.getNamespace(), edgeLabelIndex);
    }

    @Override
    public void updateMetadata() {
        // Get current list of numeric and string indexes.
        final List<FireflyIndexMetadata.IndexInfo> indexes = indexMetadata.getPropertyIndexInfos();
        final List<FireflyIndexMetadata.IndexInfo> vertexLabelIndexes = indexes.stream().
                filter(index -> index.indexType == IndexType.NUMERIC && db.LABEL_BIN.equals(index.key) && index.indexName.equals(db.V_LABEL_INDEX_NAME))
                .collect(Collectors.toList());
        final List<FireflyIndexMetadata.IndexInfo> edgeLabelIndexes = Collections.emptyList();
        final List<FireflyIndexMetadata.IndexInfo> vertexStringIndexes = indexes.stream().
                filter(index -> index.setName.equals(db.VERTEX_AERO_SET) && index.indexType == IndexType.STRING && !db.LABEL_BIN.equals(index.key)).collect(Collectors.toList());
        final List<FireflyIndexMetadata.IndexInfo> vertexNumericIndexes = indexes.stream().
                filter(index -> index.setName.equals(db.VERTEX_AERO_SET) && index.indexType == IndexType.NUMERIC && !db.LABEL_BIN.equals(index.key)).collect(Collectors.toList());
        final List<FireflyIndexMetadata.IndexInfo> edgeStringIndexes = Collections.emptyList();
        final List<FireflyIndexMetadata.IndexInfo> edgeNumericIndexes = Collections.emptyList();

        // Lock while we are changing a list that we iterate over in a different thread.
        synchronized (FireflyCardinalityMetadata.class) {
            if (!vertexLabelIndexes.isEmpty()) {
                vertexLabelCardinalityInfo = getCardinalityInfo(vertexLabelIndex, null);
            } else {
                vertexLabelCardinalityInfo = null;
            }
            if (!edgeLabelIndexes.isEmpty()) {
                edgeLabelCardinalityInfo = getCardinalityInfo(edgeLabelIndex, null);
            } else {
                edgeLabelCardinalityInfo = null;
            }
            vertexStringPropertyCardinalityInfo = vertexStringIndexes.stream().map(idx ->
                getCardinalityInfo(String.format(infoQueryFormat, db.getNamespace(), idx.indexName), idx.key)).collect(Collectors.toList());
            vertexNumericPropertyCardinalityInfo = vertexNumericIndexes.stream().map(idx ->
                getCardinalityInfo(String.format(infoQueryFormat, db.getNamespace(), idx.indexName), idx.key)).collect(Collectors.toList());
            edgeStringPropertyCardinalityInfo = edgeStringIndexes.stream().map(idx ->
                    getCardinalityInfo(String.format(infoQueryFormat, db.getNamespace(), idx.indexName), idx.key)).collect(Collectors.toList());
            edgeNumericPropertyCardinalityInfo = edgeNumericIndexes.stream().map(idx ->
                    getCardinalityInfo(String.format(infoQueryFormat, db.getNamespace(), idx.indexName), idx.key)).collect(Collectors.toList());

            System.out.println("Vertex property cardinality info: " + vertexStringPropertyCardinalityInfo);
        }
    }

    /**
     * Function to generate an optional from CardinalityInfo. Note, CardinalityInfo has a valid flag that may be false if there
     * is no data associated with the index.
     *
     * @param cardinalityInfo The CardinalityInfo to generate the optional from.
     * @return An optional containing the CardinalityInfo if it is not null, otherwise an empty optional.
     */
    private static Optional<CardinalityInfo> getValidOptionalCardinalityInfo(final CardinalityInfo cardinalityInfo) {
        // Lock while we generate the optional
        if (cardinalityInfo == null) {
            return Optional.empty();
        }
        return Optional.of(cardinalityInfo);
    }

    /**
     * Function to get vertex label cardinality of the graph.
     *
     * @return Optional containing the cardinality info if it is valid, otherwise empty.
     */
    public Optional<CardinalityInfo> getVertexLabelCardinality() {
        return getValidOptionalCardinalityInfo(vertexLabelCardinalityInfo);
    }

    /**
     * Function to get edge label cardinality of the graph.
     *
     * @return Optional containing the cardinality info if it is valid, otherwise empty.
     */
    public Optional<CardinalityInfo> getEdgeLabelCardinality() {
        return getValidOptionalCardinalityInfo(edgeLabelCardinalityInfo);
    }

    /**
     * Function to get cardinality of a given vertex property with the provided index type.
     *
     * @param property  property name.
     * @param indexType index type.
     * @return cardinality info.
     */
    public Optional<CardinalityInfo> getVertexPropertyCardinality(final String property, final IndexType indexType) {
        // Need to lock because we are iterating over a list.
        synchronized (FireflyCardinalityMetadata.class) {
            if (indexType == IndexType.STRING) {
                return getValidOptionalCardinalityInfo(vertexStringPropertyCardinalityInfo.stream().filter(cardinalityInfo -> cardinalityInfo.property.equals(property)).findFirst().orElse(null));
            } else if (indexType == IndexType.NUMERIC) {
                return getValidOptionalCardinalityInfo(vertexNumericPropertyCardinalityInfo.stream().filter(cardinalityInfo -> cardinalityInfo.property.equals(property)).findFirst().orElse(null));
            } else {
                throw new IllegalArgumentException("Cannot get vertex property cardinality for index type: " + indexType + ". Only STRING and NUMERIC are supported.");
            }
        }
    }

    /**
     * Function to get cardinality of a given edge property with the provided index type.
     *
     * @param property  property name.
     * @param indexType index type.
     * @return cardinality info.
     */
    public Optional<CardinalityInfo> getEdgePropertyCardinality(final String property, final IndexType indexType) {
        // Need to lock because we are iterating over a list.
        synchronized (FireflyCardinalityMetadata.class) {
            if (indexType == IndexType.STRING) {
                return getValidOptionalCardinalityInfo(edgeStringPropertyCardinalityInfo.stream().filter(cardinalityInfo -> cardinalityInfo.property.equals(property)).findFirst().orElse(null));
            } else if (indexType == IndexType.NUMERIC) {
                return getValidOptionalCardinalityInfo(edgeNumericPropertyCardinalityInfo.stream().filter(cardinalityInfo -> cardinalityInfo.property.equals(property)).findFirst().orElse(null));
            } else {
                throw new IllegalArgumentException("Cannot get edge property cardinality for index type: " + indexType + ". Only STRING and NUMERIC are supported.");
            }
        }
    }

    private CardinalityInfo getCardinalityInfo(final String infoVar, final String indexName) {
        final int nodeCount = this.db.getNodeCount();
        final String info = AerospikeConnection.InfoOps.singleNodeInfoRequest(this.db, infoVar);
        try {
            // Use previous failure flag to make sure we don't spam the log. If it fails, print it once, then if it starts working and failing again, print it again.
            final CardinalityInfo cardinalityInfo = new CardinalityInfo(getValue(info, ENTRIES) * nodeCount, getValue(info, ENTRIES_PER_BVAL) * nodeCount, indexName);
            previousFailure = "";
            return cardinalityInfo;
        } catch (final Exception e) {
            // Invalid.
            if (!e.getMessage().equals(previousFailure)) {
                // This happens a lot while the system gets going, it isn't really a problem, so don't spam the log.
                LOG.debug("Failed to get cardinality info from {}.", info, e);
            }
            previousFailure = e.getMessage();
            return new CardinalityInfo();
        }
    }

    public List<String> getVertexPropertyIndexes() {
        return vertexStringPropertyCardinalityInfo.stream().map(cardinalityInfo -> cardinalityInfo.property).collect(Collectors.toList());
    }

    public boolean getVertexLabelIndexExists() {
        return vertexLabelCardinalityInfo != null;
    }

    private Long getValue(final String info, final String pattern) {
        for (final String s : info.split(";")) {
            if (s.startsWith(pattern)) {
                return Long.parseLong(s.split(pattern)[1]);
            }
        }
        if ("FAIL:201:no-index".equals(info)) {
            throw new RuntimeException("Failed to find index.");
        }
        throw new RuntimeException(String.format("Error, failed to find pattern %s inside string %s.", pattern, info));
    }

    public static class CardinalityInfo {
        public final boolean valid;
        public final Long totalEntries;
        public final Long entriesPerBval;
        public final String property;

        public CardinalityInfo() {
            this.valid = false;
            this.totalEntries = null;
            this.entriesPerBval = null;
            this.property = null;
        }

        public CardinalityInfo(final long totalEntries, final long entriesPerBval, final String property) {
            this.valid = entriesPerBval != 0;
            this.totalEntries = totalEntries;
            this.entriesPerBval = entriesPerBval;
            this.property = property;
        }

        public Long getCardinality() {
            if (!valid || totalEntries == null || entriesPerBval == null) {
                return null;
            }
            // Entries per bval = total entries / cardinality.
            if (entriesPerBval == 0) {
                return 0L;
            }
            return totalEntries / entriesPerBval;
        }

        @Override
        public String toString() {
            return "CardinalityInfo{" +
                    "valid=" + valid +
                    ", totalEntries=" + totalEntries +
                    ", entriesPerBval=" + entriesPerBval +
                    ", property='" + property + '\'' +
                    '}';
        }
    }
}
