package com.aerospike.firefly.structure.util;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.io.AerospikeConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflySummaryUpdater implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(FireflySummaryUpdater.class);
    private static final int BLOCKING_QUEUE_SIZE = 25000;
    private static final int WAIT_TIME_BETWEEN_FIRST_WRITE_AND_BATCH_WRITE = 500;
    private static final int HIGH_WATERMARK = 1000;
    private static final String V_SUMMARY_RECORD = "~V_SUMMARY";
    private static final String E_SUMMARY_RECORD = "~E_SUMMARY";
    private static final String VP_PROPERTY_PREFIX = "~VP_";
    private static final String EP_PROPERTY_PREFIX = "~EP_";
    private static final String SUMMARY_LABEL_BIN = "L_SUM_BIN";
    private static final String SUMMARY_PROPERTY_BIN = "P_SUM_BIN";
    private final AerospikeConnection db;
    private final BlockingQueue<UpdateInfo> queue = new LinkedBlockingQueue<>(BLOCKING_QUEUE_SIZE);
    private final ExecutorService executorService = Executors.newSingleThreadExecutor();
    private final AtomicBoolean exited = new AtomicBoolean(false);

    // Store these locally so that we don't create any that we have already created.
    private final Map<String, Set<String>> vertexLabelToProperties = new HashMap<>();
    private final Map<String, Set<String>> edgeLabelToProperties = new HashMap<>();

    public boolean exited() {
        return this.exited.get();
    }

    public static class VertexCountInfo extends LabelCountInfo {

        private VertexCountInfo(final String label, final long count, final Set<String> properties) {
            super(label, count, properties);
        }

        @Override
        public String toString() {
            return "VertexCountInfo{label='" + label + "',count=" + count + "}";
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final VertexCountInfo that = (VertexCountInfo) o;
            return label.equals(that.label);
        }
    }

    public static class EdgeCountInfo extends LabelCountInfo {

        private EdgeCountInfo(final String label, final long count, final Set<String> properties) {
            super(label, count, properties);
        }

        @Override
        public String toString() {
            return "EdgeCountInfo{label='" + label + "',count=" + count + "}";
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final EdgeCountInfo that = (EdgeCountInfo) o;
            return label.equals(that.label);
        }
    }

    public static class LabelCountInfo implements UpdateInfo {
        public final String label;
        public long count;
        public Set<String> properties;

        private LabelCountInfo(final String label, final long count, final Set<String> properties) {
            this.label = label;
            this.count = count;
            this.properties = new HashSet<>(properties);
        }

        public void mergeOther(final LabelCountInfo other) {
            if (other == null) {
                return;
            }
            count += other.count;
            properties.addAll(other.properties);
        }

        @Override
        public int hashCode() {
            return label.hashCode();
        }

        @Override
        public String toString() {
            return "LabelCountInfo{label='" + label + "',count=" + count + "}";
        }
    }

    private static class KeyCount {
        public final String key;
        public final long count;

        private KeyCount(final String key, final long count) {
            this.key = key;
            this.count = count;
        }

        @Override
        public String toString() {
            return "KeyCount{key=" + key + ",count=" + count + "}";
        }

        @Override
        public int hashCode() {
            return key.hashCode();
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final KeyCount that = (KeyCount) o;
            return key.equals(that.key);
        }
    }

    private static class PoisonPill implements UpdateInfo {
    }

    interface UpdateInfo {
    }

    private final Key V_SUMMARY_KEY;
    private final Key E_SUMMARY_KEY;
    private final Key VP_SUMMARY_KEY;
    private final Key EP_SUMMARY_KEY;

    public FireflySummaryUpdater(final AerospikeConnection db) {
        this.db = db;
        this.VP_SUMMARY_KEY = new Key(db.getNamespace(), db.SUMMARY_SET, VP_PROPERTY_PREFIX + SUMMARY_PROPERTY_BIN);
        this.EP_SUMMARY_KEY = new Key(db.getNamespace(), db.SUMMARY_SET, EP_PROPERTY_PREFIX + SUMMARY_PROPERTY_BIN);
        this.V_SUMMARY_KEY = new Key(db.getNamespace(), db.SUMMARY_SET, V_SUMMARY_RECORD);
        this.E_SUMMARY_KEY = new Key(db.getNamespace(), db.SUMMARY_SET, E_SUMMARY_RECORD);
        this.executorService.submit(getUpdateRunnable());
    }

    public void truncate() {
        synchronized (FireflySummaryUpdater.class) {
            vertexLabelToProperties.clear();
            edgeLabelToProperties.clear();
        }
    }

    /**
     * Update the number of vertices or edges that exist under the provided label in the summary record.
     *
     * @param label      The label of the vertex or edge to update.
     * @param properties The properties of the vertex to update.
     */
    public void addVertexWriteToQueue(final String label, final Set<String> properties) {
        // If the queue is full, drop the update and let the summary skew a little.
        if (!queue.offer(new VertexCountInfo(label, 1, properties))) {
            LOG.warn("The metadata queue is full. Dropping update for vertex with label {}. Note this will" +
                    " cause summary metadata skew.", label);
        }
    }

    /**
     * Update the number of vertices or edges that exist under the provided label in the summary record.
     *
     * @param label      The label of the vertex or edge to update.
     * @param properties The properties of the edge to update.
     */
    public void addEdgeWriteToQueue(final String label, final Set<String> properties) {
        // If the queue is full, drop the update and let the summary skew a little.
        if (!queue.offer(new EdgeCountInfo(label, 1, properties))) {
            LOG.warn("The metadata queue is full. Dropping update for edge with label {}. Note this will" +
                    " cause summary metadata skew.", label);
        }
    }

    /**
     * Update the properties of an existing vertex (therefore do not update count).
     *
     * @param label      The label of the vertex or edge to update.
     * @param properties The properties of the vertex to update.
     */
    public void addVertexPropertiesWriteToQueue(final String label, final Set<String> properties) {
        if (!queue.offer(new VertexCountInfo(label, 0, properties))) {
            LOG.warn("The metadata queue is full. Dropping update for vertex properties {} with vertex label {}. Note this will" +
                    " cause summary metadata skew.", properties, label);
        }
    }

    /**
     * Update the properties of an existing edge (therefore do not update count).
     *
     * @param label      The label of the edge to update.
     * @param properties The properties of the edge to update.
     */
    public void addEdgePropertiesWriteToQueue(final String label, final Set<String> properties) {
        if (!queue.offer(new EdgeCountInfo(label, 0, properties))) {
            LOG.warn("The metadata queue is full. Dropping update for edge properties {} with edge label {}. Note this will" +
                    " cause summary metadata skew.", properties, label);
        }
    }

    @Override
    public void close() {
        // Add a poison pill to the queue to signal the thread to exit.
        queue.add(new PoisonPill());

        try {
            // Use shutdowNow so that the waiting is interrupted.
            executorService.shutdownNow();
            final boolean exited = executorService.awaitTermination(50, java.util.concurrent.TimeUnit.MILLISECONDS);
            if (!exited) {
                LOG.warn("The metadata updater thread did not exit after {} milliseconds. This may cause a dangling thread, but may also just be the metdata lagging behind.", WAIT_TIME_BETWEEN_FIRST_WRITE_AND_BATCH_WRITE + 50);
            }
        } catch (final InterruptedException e) {
            LOG.warn("Interrupted while waiting for summary updater to exit.");
        }
    }

    public class FireflyElementMetadata {
        public final Map<String, FireflyPropertiesAndCount> vertexInfo;
        public final Map<String, FireflyPropertiesAndCount> edgeInfo;

        public FireflyElementMetadata(final Map<String, FireflyPropertiesAndCount> vertexInfo,
                                      final Map<String, FireflyPropertiesAndCount> edgeInfo) {
            this.vertexInfo = vertexInfo;
            this.edgeInfo = edgeInfo;
        }

        /**
         * Get the number of vertices with the provided label.
         *
         * @return The number of vertices with the provided label.
         */
        public Map<String, Long> edgeCountByLabel() {
            final Map<String, Long> edgeCountByLabel = new HashMap<>();
            for (final Map.Entry<String, FireflyPropertiesAndCount> entry : edgeInfo.entrySet()) {
                edgeCountByLabel.put(entry.getKey(), entry.getValue().count);
            }
            return edgeCountByLabel;
        }

        /**
         * Get the number of edges with the provided label.
         *
         * @return The number of edges with the provided label.
         */
        public Map<String, Long> vertexCountByLabel() {
            final Map<String, Long> vertexCountByLabel = new HashMap<>();
            for (final Map.Entry<String, FireflyPropertiesAndCount> entry : vertexInfo.entrySet()) {
                vertexCountByLabel.put(entry.getKey(), entry.getValue().count);
            }
            return vertexCountByLabel;
        }

        /**
         * Get the properties of vertices with the provided label.
         *
         * @return The properties of vertices with the provided label.
         */
        public Map<String, Set<String>> vertexPropertiesByLabel() {
            final Map<String, Set<String>> vertexPropertiesByLabel = new HashMap<>();
            for (final Map.Entry<String, FireflyPropertiesAndCount> entry : vertexInfo.entrySet()) {
                vertexPropertiesByLabel.put(entry.getKey(), entry.getValue().properties);
            }
            return vertexPropertiesByLabel;
        }

        /**
         * Get the properties of edges with the provided label.
         *
         * @return The properties of edges with the provided label.
         */
        public Map<String, Set<String>> edgePropertiesByLabel() {
            final Map<String, Set<String>> edgePropertiesByLabel = new HashMap<>();
            for (final Map.Entry<String, FireflyPropertiesAndCount> entry : edgeInfo.entrySet()) {
                edgePropertiesByLabel.put(entry.getKey(), entry.getValue().properties);
            }
            return edgePropertiesByLabel;
        }

        /**
         * Get total vertex count.
         *
         * @return The total vertex count.
         */
        public long totalVertexCount() {
            long totalVertexCount = 0;
            for (final Map.Entry<String, FireflyPropertiesAndCount> entry : vertexInfo.entrySet()) {
                totalVertexCount += entry.getValue().count;
            }
            return totalVertexCount;
        }

        /**
         * Get total edge count.
         *
         * @return The total edge count.
         */
        public long totalEdgeCount() {
            long totalEdgeCount = 0;
            for (final Map.Entry<String, FireflyPropertiesAndCount> entry : edgeInfo.entrySet()) {
                totalEdgeCount += entry.getValue().count;
            }
            return totalEdgeCount;
        }
    }

    public static class FireflyPropertiesAndCount {
        public final Set<String> properties;
        public final long count;

        public FireflyPropertiesAndCount(final Set<String> properties, final long count) {
            this.properties = properties;
            this.count = count;
        }
    }

    public FireflyElementMetadata getFireflyStatistics() {
        // Grab vertex metadata.
        final Record vertexLabelSummaryRecord = db.getClient().get(null, V_SUMMARY_KEY);
        final Record vertexPropertySummaryRecord = db.getClient().get(null, VP_SUMMARY_KEY);
        final Map<String, FireflyPropertiesAndCount> vertexMetadata = new HashMap<>();
        if (vertexLabelSummaryRecord != null && vertexLabelSummaryRecord.bins.containsKey(SUMMARY_LABEL_BIN)) {
            final Map<String, Long> vertexCountByLabel = (Map<String, Long>) vertexLabelSummaryRecord.bins.get(SUMMARY_LABEL_BIN);
            final Map<String, List<String>> vertexPropertiesByLabel;
            if (vertexPropertySummaryRecord != null && vertexPropertySummaryRecord.bins.containsKey(SUMMARY_PROPERTY_BIN)) {
                vertexPropertiesByLabel = (Map<String, List<String>>) vertexPropertySummaryRecord.bins.get(SUMMARY_PROPERTY_BIN);
            } else {
                vertexPropertiesByLabel = new HashMap<>();
            }
            for (final String vertexLabel : vertexCountByLabel.keySet()) {
                if (vertexPropertiesByLabel != null) {
                    if (vertexPropertiesByLabel.containsKey(vertexLabel)) {
                        vertexMetadata.put(vertexLabel, new FireflyPropertiesAndCount(
                                new HashSet<>(vertexPropertiesByLabel.get(vertexLabel)), vertexCountByLabel.get(vertexLabel)));
                    } else {
                        vertexMetadata.put(vertexLabel, new FireflyPropertiesAndCount(
                                Set.of(), vertexCountByLabel.get(vertexLabel)));
                    }
                } else {
                    vertexMetadata.put(vertexLabel, new FireflyPropertiesAndCount(
                            Set.of(), vertexCountByLabel.get(vertexLabel)));
                }
            }
        }

        // Grab edge metadata.
        final Record edgeLabelSummaryRecord = db.getClient().get(null, E_SUMMARY_KEY);
        final Record edgePropertySummaryRecord = db.getClient().get(null, EP_SUMMARY_KEY);
        final Map<String, FireflyPropertiesAndCount> edgeMetadata = new HashMap<>();
        if (edgeLabelSummaryRecord != null && edgeLabelSummaryRecord.bins.containsKey(SUMMARY_LABEL_BIN)) {
            final Map<String, Long> edgeCountByLabel = (Map<String, Long>) edgeLabelSummaryRecord.bins.get(SUMMARY_LABEL_BIN);
            final Map<String, List<String>> edgePropertiesByLabel;
            if (edgePropertySummaryRecord != null && edgePropertySummaryRecord.bins.containsKey(SUMMARY_PROPERTY_BIN)) {
                edgePropertiesByLabel = (Map<String, List<String>>) edgePropertySummaryRecord.bins.get(SUMMARY_PROPERTY_BIN);
            } else {
                edgePropertiesByLabel = new HashMap<>();
            }
            for (final String edgeLabel : edgeCountByLabel.keySet()) {
                if (edgePropertiesByLabel != null) {
                    if (edgePropertiesByLabel.containsKey(edgeLabel)) {
                        edgeMetadata.put(edgeLabel, new FireflyPropertiesAndCount(
                                new HashSet<>(edgePropertiesByLabel.get(edgeLabel)), edgeCountByLabel.get(edgeLabel)));
                    } else {
                        edgeMetadata.put(edgeLabel, new FireflyPropertiesAndCount(
                                Set.of(), edgeCountByLabel.get(edgeLabel)));
                    }
                } else {
                    edgeMetadata.put(edgeLabel, new FireflyPropertiesAndCount(
                            Set.of(), edgeCountByLabel.get(edgeLabel)));
                }
            }
        }

        return new FireflyElementMetadata(vertexMetadata, edgeMetadata);
    }

    public Runnable getUpdateRunnable() {
        return () -> {
            boolean hadError = false;
            while (true) {
                final UpdateInfo info;
                final List<UpdateInfo> infoList = new ArrayList<>();
                try {
                    info = queue.take();
                } catch (final InterruptedException e) {
                    // queue.take() should break before the interrupted exception comes in, therefore this is unexpected.
                    if (!hadError) {
                        hadError = true;
                        LOG.error("Error while updating summary vertex", e);
                    }
                    continue;
                }

                if (info instanceof PoisonPill) {
                    // Take the poison pill.
                    LOG.info("Taking poison pill.");
                    break;
                }

                // If we have not blown over our watermark, wait for some time before we write.
                // We could blow over the watermark if the user is writing extreme amounts of data with no break,
                // and we just finished writing and then there was already more data in our queue.
                queue.drainTo(infoList);
                infoList.add(info);
                boolean foundPoisonPill = false;
                for (final UpdateInfo i : infoList) {
                    if (i instanceof PoisonPill) {
                        foundPoisonPill = true;
                        break;
                    }
                }
                try {
                    if (!foundPoisonPill && infoList.size() < HIGH_WATERMARK) {
                        Thread.sleep(WAIT_TIME_BETWEEN_FIRST_WRITE_AND_BATCH_WRITE);
                    }
                } catch (final InterruptedException ignored) {
                    // This is likely to be due to a shutdownNow call.
                }

                // Drain the rest of the queue.
                queue.drainTo(infoList);

                // Take the data from update info into a map.
                final Map<String, LabelCountInfo> vertexUpdates = new HashMap<>();
                final Map<String, LabelCountInfo> edgeUpdates = new HashMap<>();

                // Aggregate UpdateInfo into minimal updates.
                for (final UpdateInfo updateInfo : infoList) {
                    if (updateInfo instanceof PoisonPill) {
                        // Take the poison pill.
                        LOG.info("Found poison pill in middle of UpdateInfo stream, completing updates before taking the poison pill.");
                        foundPoisonPill = true;
                    } else {
                        final LabelCountInfo labelCountInfo = (LabelCountInfo) updateInfo;
                        final Map<String, LabelCountInfo> mapToUpdate = labelCountInfo instanceof VertexCountInfo ? vertexUpdates : edgeUpdates;
                        if (mapToUpdate.containsKey(labelCountInfo.label)) {
                            mapToUpdate.get(labelCountInfo.label).mergeOther(labelCountInfo);
                        } else {
                            mapToUpdate.put(labelCountInfo.label, labelCountInfo);
                        }
                    }
                }

                // Perform the write operations. If these fail put their data back into the queue.
                // Could do something fancy like exponential backoff here, but this runs two risks:
                //   1. If the failed data is not quickly placed back in queue and given an opportunity to come back
                //      around with more data aggregated together, there is a risk of queue overflow in high write
                //      volume.
                //   2. If a poison pill was injected and Firefly is waiting to exit, the exponential backoff could
                //      cause this to take a long time.
                boolean failed = false;
                if (didFunctionFail(() -> writeLabelCountOperations(new HashSet<>(vertexUpdates.values()), V_SUMMARY_KEY))) {
                    failed = true;
                    for (final LabelCountInfo labelCountInfo: vertexUpdates.values()) {
                        if (!queue.offer(info)) {
                            LOG.warn("The metadata queue is full. Dropping update for vertex with label {}. Note this will" +
                                    " cause summary metadata skew.", labelCountInfo.label);
                        }
                    }
                }

                if (didFunctionFail(() -> writeLabelCountOperations(new HashSet<>(edgeUpdates.values()), E_SUMMARY_KEY))) {
                    failed = true;
                    for (final LabelCountInfo labelCountInfo: edgeUpdates.values()) {
                        if (!queue.offer(info)) {
                            LOG.warn("The metadata queue is full. Dropping update for edge with label {}. Note this will" +
                                    " cause summary metadata skew.", labelCountInfo.label);
                        }
                    }
                }

                if (didFunctionFail(() -> writeLabelPropertiesOperations(new HashSet<>(vertexUpdates.values()), vertexLabelToProperties, VP_SUMMARY_KEY))) {
                    failed = true;
                    for (final Map.Entry<String, Set<String>> labelToProperties: vertexLabelToProperties.entrySet()) {
                        if (!queue.offer(new VertexCountInfo(labelToProperties.getKey(), 0, labelToProperties.getValue()))) {
                            LOG.warn("The metadata queue is full. Dropping update for vertex with label {}. Note this will" +
                                    " cause summary metadata skew.", labelToProperties.getKey());
                        }
                    }
                }

                if (didFunctionFail(() -> writeLabelPropertiesOperations(new HashSet<>(edgeUpdates.values()), edgeLabelToProperties, EP_SUMMARY_KEY))) {
                    failed = true;
                    for (final Map.Entry<String, Set<String>> labelToProperties: edgeLabelToProperties.entrySet()) {
                        if (!queue.offer(new VertexCountInfo(labelToProperties.getKey(), 0, labelToProperties.getValue()))) {
                            LOG.warn("The metadata queue is full. Dropping update for edge with label {}. Note this will" +
                                    " cause summary metadata skew.", labelToProperties.getKey());
                        }
                    }
                }

                // If the poison pill is found, exit the loop.
                if (foundPoisonPill) {
                    // This is worst case scenario for timing of poison pill and not having written data. Since the
                    // write failed, Aerospike could be down. Either way the loop needs to exit.
                    if (failed) {
                        LOG.warn("Failed to write all metadata to the summary vertex before taking poison pill. This may cause metadata skew.");
                    }
                    LOG.info("Taking poison pill.");
                    break;
                }
            }
            LOG.info("Exiting worker thread.");
            exited.set(true);
        };
    }

    interface Function {
        void run();
    }

    private boolean didFunctionFail(Function function) {
        try {
            function.run();
            return false;
        } catch (final Exception ignored) {
            return true;
        }
    }

    private void writeLabelCountOperations(final Set<LabelCountInfo> updates, final Key key) {
        // Standard write policy.
        final WritePolicy writePolicy = new WritePolicy();

        // Write the updates to the summary record.
        for (final LabelCountInfo countInfo : updates) {
            // Count can be 0 if we are adding a property to an existing vertex/edge.
            if (countInfo.count == 0) {
                continue;
            }

            final MapPolicy createOnlyPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.CREATE_ONLY | MapWriteFlags.NO_FAIL);
            final MapPolicy updateOnlyPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.UPDATE_ONLY | MapWriteFlags.NO_FAIL);
            final Operation createOp = MapOperation.put(createOnlyPolicy, SUMMARY_LABEL_BIN, Value.get(countInfo.label), Value.get(0));
            final Operation updateOp = MapOperation.increment(updateOnlyPolicy, SUMMARY_LABEL_BIN, Value.get(countInfo.label), Value.get(countInfo.count));
            db.getClient().operate(writePolicy, key, createOp, updateOp);
        }
    }

    private void writeLabelPropertiesOperations(final Set<LabelCountInfo> updates, final Map<String, Set<String>> propertyMappings, final Key key) {
        // Standard write policy.
        final WritePolicy writePolicy = new WritePolicy();

        // Write the updates to the summary record.
        for (final LabelCountInfo updateInfo : updates) {
            for (final String property : updateInfo.properties) {
                // Check if we have already inserted this mapping.
                synchronized (FireflySummaryUpdater.class) {
                    if (propertyMappings.containsKey(updateInfo.label) && propertyMappings.get(updateInfo.label).contains(property)) {
                        // Skip it.
                        continue;
                    }
                }

                final ListPolicy createListOnlyPolicy = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL);
                final Operation createOp = ListOperation.create(SUMMARY_PROPERTY_BIN, ListOrder.UNORDERED, false, CTX.mapKey(Value.get(updateInfo.label)));
                final Operation updateOp = ListOperation.append(createListOnlyPolicy, SUMMARY_PROPERTY_BIN, Value.get(property), CTX.mapKey(Value.get(updateInfo.label)));
                db.getClient().operate(writePolicy, key, createOp, updateOp);

                synchronized (FireflySummaryUpdater.class) {
                    if (!propertyMappings.containsKey(updateInfo.label)) {
                        propertyMappings.put(updateInfo.label, new HashSet<>());
                    }
                    propertyMappings.get(updateInfo.label).add(property);
                }
            }
        }
    }
}
