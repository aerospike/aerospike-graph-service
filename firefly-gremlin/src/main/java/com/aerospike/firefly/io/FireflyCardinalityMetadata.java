package com.aerospike.firefly.io;

import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.query.IndexType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyCardinalityMetadata implements FireflyMetadata {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyCardinalityMetadata.class);
    private static final Object LOCK = new Object();
    private static final String ENTRIES = "entries=";
    private static final String ENTRIES_PER_BVAL = "entries_per_bval=";
    private static final String infoQueryFormat = "sindex/%s/%s"; // "sindex/<namespace>/<index name>
    private CardinalityInfo vertexLabelCardinalityInfo = new CardinalityInfo();
    private CardinalityInfo edgeLabelCardinalityInfo = new CardinalityInfo();
    private List<CardinalityInfo> vertexNumericPropertyCardinalityInfo = new ArrayList<>();
    private List<CardinalityInfo> vertexStringPropertyCardinalityInfo = new ArrayList<>();
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
    public void updateMetadata() throws Exception {
        // Get Nodes.
        final Node[] nodes = db.getClient().getNodes();
        if (nodes.length < 1) {
            throw new Exception("Error, there is " + nodes.length + " nodes available. Expected at least 1.");
        }

        // Get current list of numeric and string indexes.
        final List<FireflyIndexMetadata.IndexInfo> indexes = indexMetadata.getPropertyIndexInfos();
        final List<FireflyIndexMetadata.IndexInfo> stringIndexes = indexes.stream().
                filter(index -> index.indexType == IndexType.STRING && !"label".equals(index.key)).collect(Collectors.toList());
        final List<FireflyIndexMetadata.IndexInfo> numericIndexes = indexes.stream().
                filter(index -> index.indexType == IndexType.NUMERIC && !"label".equals(index.key)).collect(Collectors.toList());

        // We can assume that data is relatively evenly distributed across nodes.
        final Node node = nodes[0];

        // Lock while we are changing a list that we iterate over in a different thread.
        synchronized (LOCK) {
            vertexLabelCardinalityInfo = getCardinalityInfo(Info.request(new InfoPolicy(), node, vertexLabelIndex), nodes.length, null);
            edgeLabelCardinalityInfo = getCardinalityInfo(Info.request(new InfoPolicy(), node, edgeLabelIndex), nodes.length, null);
            vertexStringPropertyCardinalityInfo = stringIndexes.stream().map(idx -> getCardinalityInfo(Info.request(new InfoPolicy(), node, String.format(infoQueryFormat, db.getNamespace(), idx.indexName)), nodes.length, idx.key)).collect(Collectors.toList());
            vertexNumericPropertyCardinalityInfo = numericIndexes.stream().map(idx -> getCardinalityInfo(Info.request(new InfoPolicy(), node, String.format(infoQueryFormat, db.getNamespace(), idx.indexName)), nodes.length, idx.key)).collect(Collectors.toList());
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
        synchronized (LOCK) {
            if (indexType == IndexType.STRING) {
                return getValidOptionalCardinalityInfo(vertexStringPropertyCardinalityInfo.stream().filter(cardinalityInfo -> cardinalityInfo.property.equals(property)).findFirst().orElse(null));
            } else if (indexType == IndexType.NUMERIC) {
                return getValidOptionalCardinalityInfo(vertexNumericPropertyCardinalityInfo.stream().filter(cardinalityInfo -> cardinalityInfo.property.equals(property)).findFirst().orElse(null));
            } else {
                throw new IllegalArgumentException("Cannot get vertex property cardinality for index type: " + indexType + ". Only STRING and NUMERIC are supported.");
            }
        }
    }
    // Edge property cardinality intentionally omitted here. We need to switch those over to configs and properly implement it.
    // Note - when this is added, TestFireflyMetadata should also be updated.
    //     public Optional<CardinalityInfo> getEdgePropertyCardinality(final String property, final IndexType indexType) {
    //        if (indexType == IndexType.STRING) {
    //            return getValidOptionalCardinalityInfo(edgeStringPropertyCardinalityInfo.stream().filter(cardinalityInfo -> cardinalityInfo.property.equals(property)).findFirst().orElse(null));
    //        } else if (indexType == IndexType.NUMERIC) {
    //            return getValidOptionalCardinalityInfo(edgeNumericPropertyCardinalityInfo.stream().filter(cardinalityInfo -> cardinalityInfo.property.equals(property)).findFirst().orElse(null));
    //        } else {
    //            throw new IllegalArgumentException("Cannot get vertex property cardinality for index type: " + indexType". Only STRING and NUMERIC are supported.");
    //        }
    //    }

    private CardinalityInfo getCardinalityInfo(final String info, final int nodeCount, final String indexName) {
        try {
            // Use previous failure flag to make sure we don't spam the log. If it fails, print it once, then if it starts working and failing again, print it again.
            final CardinalityInfo cardinalityInfo = new CardinalityInfo(getValue(info, ENTRIES) * nodeCount, getValue(info, ENTRIES_PER_BVAL) * nodeCount, indexName);
            previousFailure = "";
            return cardinalityInfo;
        } catch (Exception e) {
            // Invalid.
            if (!e.getMessage().equals(previousFailure)) {
                // This happens a lot while the system gets going, it isn't really a problem, so don't spam the log.
                LOG.debug("Failed to get cardinality info from {}.", info, e);
            }
            previousFailure = e.getMessage();
            return new CardinalityInfo();
        }
    }

    private Long getValue(final String info, final String pattern) throws Exception {
        for (final String s : info.split(";")) {
            if (s.startsWith(pattern)) {
                return Long.parseLong(s.split(pattern)[1]);
            }
        }
        if ("FAIL:201:no-index".equals(info)) {
            throw new Exception("Failed to find index.");
        }
        throw new Exception(String.format("Error, failed to find pattern %s inside string %s.", pattern, info));
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
    }
}
