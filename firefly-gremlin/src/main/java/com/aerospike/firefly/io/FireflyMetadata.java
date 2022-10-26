package com.aerospike.firefly.io;

import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

public class FireflyMetadata extends TimerTask {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyMetadata.class);
    private static final String ENTRIES = "entries=";
    private static final String ENTRIES_PER_BVAL = "entries_per_bval=";
    private static final String infoQueryFormat = "sindex/%s/%s"; // "sindex/<namespace>/<index name>
    private static Timer time = new Timer();
    private static final Object TIME_LOCK = new Object();
    public static boolean isRunning = false;
    public static CardinalityInfo vertexLabelCardinalityInfo = new CardinalityInfo();
    public static CardinalityInfo edgeLabelCardinalityInfo = new CardinalityInfo();
    public static CardinalityInfo vertexNumericPropertyCardinalityInfo = new CardinalityInfo();
    public static CardinalityInfo vertexStringPropertyCardinalityInfo = new CardinalityInfo();
    public static CardinalityInfo edgeNumericPropertyCardinalityInfo = new CardinalityInfo();
    public static CardinalityInfo edgeStringPropertyCardinalityInfo = new CardinalityInfo();
    private final AerospikeConnection db;
    private final String vertexLabelIndex;
    private final String edgeLabelIndex;
    private final String vertexStringPropertyIndex;
    private final String vertexNumericPropertyIndex;
    private final String edgeStringPropertyIndex;
    private final String edgeNumericPropertyIndex;

    private FireflyMetadata(final AerospikeConnection db,
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

    public static void startPeriodicUpdates(final AerospikeConnection db,
                                            final String vertexLabelIndex,
                                            final String edgeLabelIndex,
                                            final String vertexNumericPropertyIndex,
                                            final String vertexStringPropertyIndex,
                                            final String edgeNumericPropertyIndex,
                                            final String edgeStringPropertyIndex,
                                            final long updateFrequency) {
        // Update every hour. This is from bval statistics specification in Aerospike. https://docs.aerospike.com/reference/metrics#entries_per_bval
        synchronized (TIME_LOCK) {
            time.schedule(
                    new FireflyMetadata(db,
                            vertexLabelIndex,
                            edgeLabelIndex,
                            vertexNumericPropertyIndex,
                            vertexStringPropertyIndex,
                            edgeNumericPropertyIndex,
                            edgeStringPropertyIndex),
                    0, // No delay.
                    updateFrequency);
            isRunning = true;
        }
    }

    public static void stopPeriodicUpdates() {
        synchronized (TIME_LOCK) {
            if (isRunning) {
                time.cancel();
                time = new Timer();
                isRunning = false;
            }
        }
    }

    // Periodic execution.
    public void run() {
        try {
            updateMetadata();
        } catch (Exception ex) {
            LOG.error("Error in FireflyMetadata update thread:", ex);
        }
    }

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
            return new CardinalityInfo(getValue(info, ENTRIES) * nodeCount, getValue(info, ENTRIES_PER_BVAL) * nodeCount);
        } catch (Exception e) {
            // Invalid.
            LOG.error("Failed to get cardinality info from {}.", info, e);
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
