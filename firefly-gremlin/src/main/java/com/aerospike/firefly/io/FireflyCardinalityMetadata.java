package com.aerospike.firefly.io;

import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyCardinalityMetadata implements FireflyMetadata {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyCardinalityMetadata.class);
    private static final String ENTRIES = "entries=";
    private static final String ENTRIES_PER_BVAL = "entries_per_bval=";
    private static final String infoQueryFormat = "sindex/%s/%s"; // "sindex/<namespace>/<index name>
    private static final Object TIME_LOCK = new Object();
    public static boolean isRunning = false;
    public CardinalityInfo vertexLabelCardinalityInfo = new CardinalityInfo();
    public CardinalityInfo edgeLabelCardinalityInfo = new CardinalityInfo();
    public CardinalityInfo vertexNumericPropertyCardinalityInfo = new CardinalityInfo();
    public CardinalityInfo vertexStringPropertyCardinalityInfo = new CardinalityInfo();
    public CardinalityInfo edgeNumericPropertyCardinalityInfo = new CardinalityInfo();
    public CardinalityInfo edgeStringPropertyCardinalityInfo = new CardinalityInfo();
    private final AerospikeConnection db;
    private final String vertexLabelIndex;
    private final String edgeLabelIndex;
    private final String vertexStringPropertyIndex;
    private final String vertexNumericPropertyIndex;
    private final String edgeStringPropertyIndex;
    private final String edgeNumericPropertyIndex;

    private String previousFailure;

    public FireflyCardinalityMetadata(final AerospikeConnection db,
                            final String vertexLabelIndex,
                            final String edgeLabelIndex,
                            final String vertexNumericPropertyIndex,
                            final String vertexStringPropertyIndex,
                            final String edgeNumericPropertyIndex,
                            final String edgeStringPropertyIndex) {
        this.vertexLabelIndex = String.format(infoQueryFormat, db.getNamespace(), vertexLabelIndex);
        this.edgeLabelIndex = String.format(infoQueryFormat, db.getNamespace(), edgeLabelIndex);
        this.vertexNumericPropertyIndex = String.format(infoQueryFormat, db.getNamespace(), vertexNumericPropertyIndex);
        this.vertexStringPropertyIndex = String.format(infoQueryFormat, db.getNamespace(), vertexStringPropertyIndex);
        this.edgeNumericPropertyIndex = String.format(infoQueryFormat, db.getNamespace(), edgeNumericPropertyIndex);
        this.edgeStringPropertyIndex = String.format(infoQueryFormat, db.getNamespace(), edgeStringPropertyIndex);
        this.db = db;
    }

    @Override
    public void updateMetadata() throws Exception {
        // Get Nodes.
        final Node[] nodes = db.getClient().getNodes();
        if (nodes.length < 1) {
            throw new Exception("Error, there is " + nodes.length + " nodes available. Expected at least 1.");
        }

        // We can assume that data is relatively evenly distributed across nodes.
        final Node node = nodes[0];
        vertexLabelCardinalityInfo = getCardinalityInfo(Info.request(new InfoPolicy(), node, vertexLabelIndex), nodes.length);
        edgeLabelCardinalityInfo = getCardinalityInfo(Info.request(new InfoPolicy(), node, edgeLabelIndex), nodes.length);
        vertexNumericPropertyCardinalityInfo = getCardinalityInfo(Info.request(new InfoPolicy(), node, vertexNumericPropertyIndex), nodes.length);
        vertexStringPropertyCardinalityInfo = getCardinalityInfo(Info.request(new InfoPolicy(), node, vertexStringPropertyIndex), nodes.length);
        edgeNumericPropertyCardinalityInfo = getCardinalityInfo(Info.request(new InfoPolicy(), node, edgeNumericPropertyIndex), nodes.length);
        edgeStringPropertyCardinalityInfo = getCardinalityInfo(Info.request(new InfoPolicy(), node, edgeStringPropertyIndex), nodes.length);
    }

    private CardinalityInfo getCardinalityInfo(final String info, final int nodeCount) {
        try {
            // Use previous failure flag to make sure we don't spam the log. If it fails, print it once, then if it starts working and failing again, print it again.
            final CardinalityInfo cardinalityInfo = new CardinalityInfo(getValue(info, ENTRIES) * nodeCount, getValue(info, ENTRIES_PER_BVAL) * nodeCount);
            previousFailure = "";
            return cardinalityInfo;
        } catch (Exception e) {
            // Invalid.
            if (!e.getMessage().equals(previousFailure)) {
                LOG.error("Failed to get cardinality info from {}.", info, e);
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
        throw new Exception(String.format("Error, failed to find pattern %s inside string %s.", pattern, info));
    }

    public static class CardinalityInfo {
        public final boolean valid;
        public final Long totalEntries;
        public final Long entriesPerBval;

        public CardinalityInfo() {
            valid = false;
            totalEntries = null;
            entriesPerBval = null;
        }

        public CardinalityInfo(long totalEntries, long entriesPerBval) {
            valid = entriesPerBval != 0;
            this.totalEntries = totalEntries;
            this.entriesPerBval = entriesPerBval;
        }
    }
}
