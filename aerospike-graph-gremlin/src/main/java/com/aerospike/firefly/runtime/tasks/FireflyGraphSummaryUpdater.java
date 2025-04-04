package com.aerospike.firefly.runtime.tasks;

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
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static com.aerospike.firefly.process.call.metadata.MetadataServiceSummary.PRETTY_PRINT_FORMAT_LOG;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyGraphSummaryUpdater implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyGraphSummaryUpdater.class);
    private static final int HIGH_WATERMARK = 250;
    public static final int MAP_RECYCLE_SIZE = 5000;
    private static final int LATCH_BREAK_TIME_MILLISECONDS = 1000;
    private static final String V_SUMMARY_RECORD = "~V_SUMMARY";
    private static final String E_SUMMARY_RECORD = "~E_SUMMARY";
    private static final String VP_PROPERTY_PREFIX = "~VP_";
    private static final String EP_PROPERTY_PREFIX = "~EP_";
    private static final String SUMMARY_LABEL_BIN = "L_SUM_BIN";
    private static final String SUMMARY_PROPERTY_BIN = "P_SUM_BIN";
    private final AerospikeConnection db;
    private final AtomicBoolean EXITED = new AtomicBoolean(false);
    private CountDownLatch COUNTDOWN_LATCH = new CountDownLatch(HIGH_WATERMARK);
    private CountDownLatch SHUTDOWN_LATCH = new CountDownLatch(1);
    private final AtomicBoolean SHUTDOWN = new AtomicBoolean(false);
    private static final Map<String, AerospikeGraphException> LAST_SUMMARY_TICKER_EXCEPTION = new ConcurrentHashMap<>();

    // Create as daemon so it exits with process.
    private final ExecutorService EXECUTOR_SERVICE = Executors.newFixedThreadPool(1,
            r -> {
                Thread t = Executors.defaultThreadFactory().newThread(r);
                t.setDaemon(true);
                return t;
            });
    private final AtomicLong RUNNING_COUNT = new AtomicLong(0);

    // Store these locally so that we don't create any that we have already created.
    private final Map<String, Set<String>> vertexLabelToProperties = new HashMap<>();
    private final Map<String, Set<String>> edgeLabelToProperties = new HashMap<>();
    private final AtomicLong lastTickerOutputTime = new AtomicLong(0);

    public boolean exited() {
        return EXITED.get();
    }

    // These are public strictly for testing. Don't mess with them outside of this class.
    public Map<String, Set<String>> edgeProperties = new ConcurrentHashMap<>();
    public Map<String, Set<String>> vertexProperties = new ConcurrentHashMap<>();
    public Map<String, AtomicLong> edgeCounts = new ConcurrentHashMap<>();
    public Map<String, AtomicLong> vertexCounts = new ConcurrentHashMap<>();
    public Map<Integer, Map<String, AtomicLong>> vertexPartitionCounts = new ConcurrentHashMap<>();
    public Map<Integer, Map<String, AtomicLong>> edgePartitionCounts = new ConcurrentHashMap<>();

    private final Key V_SUMMARY_KEY;
    private final Key E_SUMMARY_KEY;
    private final Key VP_SUMMARY_KEY;
    private final Key EP_SUMMARY_KEY;
    private final AtomicBoolean TRUNCATION = new AtomicBoolean(false);

    public static class LabelCountInfo implements UpdateInfo {
        public final String label;
        public long count;
        public Set<String> properties;

        private LabelCountInfo(final String label, final long count, final Set<String> properties) {
            this.label = label;
            this.count = count;
            this.properties = new HashSet<>(properties);
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

    interface UpdateInfo {
    }

    public FireflyGraphSummaryUpdater(final AerospikeConnection db) {
        this.db = db;
        this.VP_SUMMARY_KEY = new Key(db.getNamespace(), db.SUMMARY_SET, VP_PROPERTY_PREFIX + SUMMARY_PROPERTY_BIN);
        this.EP_SUMMARY_KEY = new Key(db.getNamespace(), db.SUMMARY_SET, EP_PROPERTY_PREFIX + SUMMARY_PROPERTY_BIN);
        this.V_SUMMARY_KEY = new Key(db.getNamespace(), db.SUMMARY_SET, V_SUMMARY_RECORD);
        this.E_SUMMARY_KEY = new Key(db.getNamespace(), db.SUMMARY_SET, E_SUMMARY_RECORD);
        if (db.SUMMARY_ENABLED_FLAG) {
            synchronized (EXECUTOR_SERVICE) {
                if (RUNNING_COUNT.addAndGet(1) == 1) {
                    EXITED.set(false);
                    SHUTDOWN.set(false);
                    EXECUTOR_SERVICE.submit(getUpdateRunnable());
                }
                final List<String> setIndex = AerospikeConnection.InfoOps.createSetIndex(db, db.SUMMARY_SET);
                for (final String index : setIndex) {
                    if (!"ok".equals(index)) {
                        LOG.error("Error creating set index: {}", index);
                    }
                }
            }
        }
    }

    public void truncate() {
        TRUNCATION.set(true);
        synchronized (FireflyGraphSummaryUpdater.class) {
            // Need to nuke the queue otherwise it will continue spilling out updates after we truncate.
            vertexLabelToProperties.clear();
            edgeLabelToProperties.clear();
            vertexProperties.clear();
            edgeProperties.clear();
            vertexCounts.clear();
            edgeCounts.clear();
            vertexPartitionCounts.clear();
            edgePartitionCounts.clear();
            TRUNCATION.set(false);
        }
    }

    /**
     * Update the number of vertices that exist under the provided label in the summary record.
     *
     * @param label      The label of the vertex to update.
     * @param properties The properties of the vertex to update.
     */
    public void addVertexWriteToQueue(final String label, final Set<String> properties) {
        if (!db.SUMMARY_ENABLED_FLAG) {
            return;
        }

        vertexCounts.computeIfAbsent(label, k -> new AtomicLong(0));
        vertexCounts.get(label).addAndGet(1);
        vertexProperties.computeIfAbsent(label, k -> ConcurrentHashMap.newKeySet());
        vertexProperties.get(label).addAll(properties);
        COUNTDOWN_LATCH.countDown();
    }

    /**
     * Stage an update to the number of vertices that exist under the provided label in the summary record.
     *
     * @param label      The label of the vertex to update.
     * @param properties The properties of the vertex to update.
     */
    public void stageVertexWriteToQueue(final String label, final Set<String> properties, final int partitionId) {
        if (!db.SUMMARY_ENABLED_FLAG) {
            return;
        }

        vertexPartitionCounts.computeIfAbsent(partitionId, k -> new ConcurrentHashMap<>());
        vertexPartitionCounts.get(partitionId).computeIfAbsent(label, k -> new AtomicLong(0));
        vertexPartitionCounts.get(partitionId).get(label).addAndGet(1);
        vertexProperties.computeIfAbsent(label, k -> ConcurrentHashMap.newKeySet());
        vertexProperties.get(label).addAll(properties);
        COUNTDOWN_LATCH.countDown();
    }

    /**
     * Update the number of vertices that exist under the provided label in the summary record.
     *
     * @param label The label of the vertex to remove.
     */
    public void addVertexRemoveToQueue(final String label) {
        if (!db.SUMMARY_ENABLED_FLAG) {
            return;
        }

        vertexCounts.computeIfAbsent(label, k -> new AtomicLong(0));
        vertexCounts.get(label).addAndGet(-1);
        COUNTDOWN_LATCH.countDown();
    }

    /**
     * Update the number of edges that exist under the provided label in the summary record.
     *
     * @param label      The label of the edge to update.
     * @param properties The properties of the edge to update.
     */
    public void addEdgeWriteToQueue(final String label, final Set<String> properties) {
        if (!db.SUMMARY_ENABLED_FLAG) {
            return;
        }

        edgeCounts.computeIfAbsent(label, k -> new AtomicLong(0));
        edgeCounts.get(label).addAndGet(1);
        edgeProperties.computeIfAbsent(label, k -> ConcurrentHashMap.newKeySet());
        edgeProperties.get(label).addAll(properties);
        COUNTDOWN_LATCH.countDown();
    }

    /**
     * Stage an update the number of edges that exist under the provided label in the summary record.
     *
     * @param label      The label of the edge to update.
     * @param properties The properties of the edge to update.
     */
    public void stageEdgeWriteToQueue(final String label, final Set<String> properties, final int partitionId) {
        if (!db.SUMMARY_ENABLED_FLAG) {
            return;
        }

        edgePartitionCounts.computeIfAbsent(partitionId, k -> new ConcurrentHashMap<>());
        edgePartitionCounts.get(partitionId).computeIfAbsent(label, k -> new AtomicLong(0));
        edgePartitionCounts.get(partitionId).get(label).addAndGet(1);
        edgeProperties.computeIfAbsent(label, k -> ConcurrentHashMap.newKeySet());
        edgeProperties.get(label).addAll(properties);
        COUNTDOWN_LATCH.countDown();
    }

    public void startVertexPartition(final int partitionId) {
        LOG.info("start vertex partition: {}", partitionId);
        if (!db.SUMMARY_ENABLED_FLAG) {
            return;
        }

        // Need to kill what is in Aerospike because this is from a failed partition if it exists.
        final Key vertexPartitionKey = getVertexPartitionKey(partitionId);
        db.delete(vertexPartitionKey, null);
    }

    public void completeVertexPartition(final int partitionId) {
        if (!db.SUMMARY_ENABLED_FLAG) {
            return;
        }

        // Need to get what is in Aerospike for this partition and move it to the main dataset.
        final Key vertexPartitionKey = getVertexPartitionKey(partitionId);
        final Map<String, Long> labelToCount = getPartitionCounts(vertexPartitionKey);
        if (vertexPartitionCounts.containsKey(partitionId)) {
            for (final String label : vertexPartitionCounts.get(partitionId).keySet()) {
                vertexCounts.computeIfAbsent(label, k -> new AtomicLong(0));
                vertexCounts.get(label).addAndGet(vertexPartitionCounts.get(partitionId).get(label).get());
            }
        }
        for (final String label : labelToCount.keySet()) {
            vertexCounts.computeIfAbsent(label, k -> new AtomicLong(0));
            vertexCounts.get(label).addAndGet(labelToCount.get(label));
        }
        LOG.info("complete vertex partition: {}, vertexCounts: {}", partitionId, vertexCounts);
        db.delete(vertexPartitionKey, null);
        while (COUNTDOWN_LATCH.getCount() > 0) {
            COUNTDOWN_LATCH.countDown();
        }
    }

    public void startEdgePartition(final int partitionId) {
        LOG.info("start edge partition: {}", partitionId);
        if (!db.SUMMARY_ENABLED_FLAG) {
            return;
        }
        // Need to kill what is in Aerospike because this is from a failed partition if it exists.
        final Key edgePartitionKey = getEdgePartitionKey(partitionId);
        db.delete(edgePartitionKey, null);
    }

    public void completeEdgePartition(final int partitionId) {
        if (!db.SUMMARY_ENABLED_FLAG) {
            return;
        }

        // Need to get what is in Aerospike for this partition and move it to the main dataset.
        final Key edgePartitionKey = getEdgePartitionKey(partitionId);
        final Map<String, Long> labelToCount = getPartitionCounts(edgePartitionKey);
        if (edgePartitionCounts.containsKey(partitionId)) {
            for (final String label : edgePartitionCounts.get(partitionId).keySet()) {
                edgeCounts.computeIfAbsent(label, k -> new AtomicLong(0));
                edgeCounts.get(label).addAndGet(edgePartitionCounts.get(partitionId).get(label).get());
            }
        }
        for (final String label : labelToCount.keySet()) {
            edgeCounts.computeIfAbsent(label, k -> new AtomicLong(0));
            edgeCounts.get(label).addAndGet(labelToCount.get(label));
        }
        LOG.info("complete edge partition: {}, edgeCounts: {}", partitionId, edgeCounts);
        db.delete(edgePartitionKey, null);
        while (COUNTDOWN_LATCH.getCount() > 0) {
            COUNTDOWN_LATCH.countDown();
        }
    }

    private Key getEdgePartitionKey(final int partitionId) {
        return new Key(db.getNamespace(), db.SUMMARY_SET, EP_PROPERTY_PREFIX + "PART_" + partitionId);
    }

    private Key getVertexPartitionKey(final int partitionId) {
        return new Key(db.getNamespace(), db.SUMMARY_SET, VP_PROPERTY_PREFIX + "PART_" + partitionId);
    }

    private Map<String, Long> getPartitionCounts(final Key key) {
        final Record record = db.read(key, null);

        if (record != null && record.bins.containsKey(SUMMARY_LABEL_BIN)) {
            return (Map<String, Long>) record.bins.get(SUMMARY_LABEL_BIN);
        } else {
            return new HashMap<>();
        }
    }

    /**
     * Update the number of vertices or edges that exist under the provided label in the summary record.
     *
     * @param label      The label of the vertex or edge to update.
     */
    public void addEdgeRemoveToQueue(final String label) {
        if (!db.SUMMARY_ENABLED_FLAG) {
            return;
        }

        edgeCounts.computeIfAbsent(label, k -> new AtomicLong(0));
        edgeCounts.get(label).addAndGet(-1);
        COUNTDOWN_LATCH.countDown();
    }

    /**
     * Update the properties of an existing vertex (therefore do not update count).
     *
     * @param label      The label of the vertex or edge to update.
     * @param properties The properties of the vertex to update.
     */
    public void addVertexPropertiesWriteToQueue(final String label, final Set<String> properties) {
        if (!db.SUMMARY_ENABLED_FLAG) {
            return;
        }

        vertexProperties.computeIfAbsent(label, k -> ConcurrentHashMap.newKeySet());
        vertexProperties.get(label).addAll(properties);
        COUNTDOWN_LATCH.countDown();
    }

    /**
     * Update the properties of an existing edge (therefore do not update count).
     *
     * @param label      The label of the edge to update.
     * @param properties The properties of the edge to update.
     */
    public void addEdgePropertiesWriteToQueue(final String label, final Set<String> properties) {
        if (!db.SUMMARY_ENABLED_FLAG) {
            return;
        }

        edgeProperties.computeIfAbsent(label, k -> ConcurrentHashMap.newKeySet());
        edgeProperties.get(label).addAll(properties);
        COUNTDOWN_LATCH.countDown();
    }

    @Override
    public void close() {
        if (!db.SUMMARY_ENABLED_FLAG) {
            return;
        }

        synchronized (EXECUTOR_SERVICE) {
            // If all firefly instances are not closed, exit.
            if (RUNNING_COUNT.addAndGet(-1) != 0) {
                return;
            }
        }

        SHUTDOWN_LATCH = new CountDownLatch(1);
        SHUTDOWN.set(true);

        // Just in case the thread is waiting for the countdown to finish, drive it to 0.
        while (COUNTDOWN_LATCH.getCount() > 0) {
            COUNTDOWN_LATCH.countDown();
        }

        try {
            // Wait for the shutdown latch.
            final boolean exited = SHUTDOWN_LATCH.await(3 * LATCH_BREAK_TIME_MILLISECONDS, TimeUnit.MILLISECONDS);
            if (!exited) {
                LOG.warn("The metadata updater thread did not exit after {} milliseconds. " +
                                "This may just be the metadata lagging behind, however since it is a daemon we can exit anyway.",
                        3 * LATCH_BREAK_TIME_MILLISECONDS);
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            LOG.warn("Interrupted while waiting for summary updater to exit.");
        }

        if (!vertexCounts.isEmpty() || !edgeCounts.isEmpty())
            doWrite();
    }

    /**
     * Write summary updater information to Aerospike Database.
     *
     * @return if the summary updating process should continue.
     */
    private boolean doWrite() {
        try {
            synchronized (FireflyGraphSummaryUpdater.class) {
                // Take the data from update info into a map.
                final Map<String, LabelCountInfo> vertexUpdates = new HashMap<>();
                final Map<String, LabelCountInfo> edgeUpdates = new HashMap<>();

                // At this point we are about to take all the data out of the maps and put them into a new map.
                // We did not synchronize this, but we don't care about exacts, only roughly how much is coming in,
                // so now is an appropriate time to reset the latch.
                COUNTDOWN_LATCH = new CountDownLatch(HIGH_WATERMARK);

                final Map<String, AtomicLong> vertexCountsToUse = vertexCounts;
                final Map<String, Set<String>> vertexPropertiesToUse = vertexProperties;
                if (vertexCounts.keySet().size() > MAP_RECYCLE_SIZE ||
                        vertexProperties.keySet().size() > MAP_RECYCLE_SIZE) {
                    // Ideally we don't want to have to do this, but we don't want the map to grow infinitely in the case
                    // that firefly never gets shut down and the customer for some odd reason keeps using new labels.
                    vertexCounts = new ConcurrentHashMap<>();
                    vertexProperties = new ConcurrentHashMap<>();
                }

                final Set<String> vertexKeys = new HashSet<>(vertexCountsToUse.keySet());
                vertexKeys.addAll(vertexPropertiesToUse.keySet());
                for (final String key : vertexKeys) {
                    vertexCountsToUse.computeIfAbsent(key, k -> new AtomicLong(0));
                    final long count = vertexCountsToUse.get(key).getAndSet(0);
                    final Set<String> properties = vertexPropertiesToUse.getOrDefault(key, Collections.emptySet());
                    vertexUpdates.put(key, new LabelCountInfo(key, count, properties));
                }

                final Map<String, AtomicLong> edgeCountsToUse = edgeCounts;
                final Map<String, Set<String>> edgePropertiesToUse = edgeProperties;
                if (edgeCounts.keySet().size() > MAP_RECYCLE_SIZE ||
                        edgeProperties.keySet().size() > MAP_RECYCLE_SIZE) {
                    // Ideally we don't want to have to do this, but we don't want the map to grow infinitely in the case
                    // that firefly never gets shut down and the customer for some odd reason keeps using new labels.
                    edgeCounts = new ConcurrentHashMap<>();
                    edgeProperties = new ConcurrentHashMap<>();
                }

                final Set<String> edgeKeys = new HashSet<>(edgeCountsToUse.keySet());
                edgeKeys.addAll(edgePropertiesToUse.keySet());
                for (final String key : edgeKeys) {
                    edgeCountsToUse.computeIfAbsent(key, k -> new AtomicLong(0));
                    final long count = edgeCountsToUse.get(key).getAndSet(0);
                    final Set<String> properties = edgePropertiesToUse.getOrDefault(key, Collections.emptySet());
                    edgeUpdates.put(key, new LabelCountInfo(key, count, properties));
                }

                // Perform the write operations. If these fail put their data back into the map.
                // Could do something fancy like exponential backoff here, but this runs two risks:
                //   1. If the failed data is not quickly placed back in queue and given an opportunity to come back
                //      around with more data aggregated together, there is a risk of queue overflow in high write
                //      volume.
                //   2. If a poison pill was injected and Firefly is waiting to exit, the exponential backoff could
                //      cause this to take a long time.
                boolean failed = false;
                if (didFunctionFail(() -> writeLabelCountOperations(new HashSet<>(vertexUpdates.values()), V_SUMMARY_KEY))) {
                    failed = true;
                    for (final LabelCountInfo labelCountInfo : vertexUpdates.values()) {
                        vertexCounts.putIfAbsent(labelCountInfo.label, new AtomicLong(0));
                        vertexCounts.get(labelCountInfo.label).addAndGet(labelCountInfo.count);
                    }
                }

                if (didFunctionFail(() -> writeLabelCountOperations(new HashSet<>(edgeUpdates.values()), E_SUMMARY_KEY))) {
                    failed = true;
                    for (final LabelCountInfo labelCountInfo : edgeUpdates.values()) {
                        edgeCounts.putIfAbsent(labelCountInfo.label, new AtomicLong(0));
                        edgeCounts.get(labelCountInfo.label).addAndGet(labelCountInfo.count);
                    }
                }

                if (didFunctionFail(() -> writeLabelPropertiesOperations(new HashSet<>(vertexUpdates.values()), vertexLabelToProperties, VP_SUMMARY_KEY))) {
                    failed = true;
                    // We don't need to re-insert these because they weren't removed.
                }

                if (didFunctionFail(() -> writeLabelPropertiesOperations(new HashSet<>(edgeUpdates.values()), edgeLabelToProperties, EP_SUMMARY_KEY))) {
                    failed = true;
                    // We don't need to re-insert these because they weren't removed.
                }

                for (final Integer partition : vertexPartitionCounts.keySet()) {
                    final Map<String, LabelCountInfo> partitionVertexUpdates = new HashMap<>();
                    final Map<String, AtomicLong> partitionVertexCounts = vertexPartitionCounts.getOrDefault(partition, new ConcurrentHashMap<>());
                    for (final String key : partitionVertexCounts.keySet()) {
                        final long count = partitionVertexCounts.get(key).getAndSet(0);
                        partitionVertexUpdates.put(key, new LabelCountInfo(key, count, new HashSet<>()));
                    }

                    if (didFunctionFail(() -> writeLabelCountOperations(new HashSet<>(partitionVertexUpdates.values()), getVertexPartitionKey(partition)))) {
                        failed = true;
                        for (final LabelCountInfo labelCountInfo : partitionVertexUpdates.values()) {
                            vertexPartitionCounts.putIfAbsent(partition, new ConcurrentHashMap<>());
                            vertexPartitionCounts.get(partition).putIfAbsent(labelCountInfo.label, new AtomicLong(0));
                            vertexPartitionCounts.get(partition).get(labelCountInfo.label).addAndGet(labelCountInfo.count);
                        }
                    }
                }

                for (final Integer partition : edgePartitionCounts.keySet()) {
                    final Map<String, LabelCountInfo> partitionEdgeUpdates = new HashMap<>();
                    final Map<String, AtomicLong> partitionEdgeCounts = edgePartitionCounts.getOrDefault(partition, new ConcurrentHashMap<>());
                    for (final String key : partitionEdgeCounts.keySet()) {
                        final long count = partitionEdgeCounts.get(key).getAndSet(0);
                        partitionEdgeUpdates.put(key, new LabelCountInfo(key, count, new HashSet<>()));
                    }

                    if (didFunctionFail(() -> writeLabelCountOperations(new HashSet<>(partitionEdgeUpdates.values()), getEdgePartitionKey(partition)))) {
                        failed = true;
                        for (final LabelCountInfo labelCountInfo : partitionEdgeUpdates.values()) {
                            edgePartitionCounts.putIfAbsent(partition, new ConcurrentHashMap<>());
                            edgePartitionCounts.get(partition).putIfAbsent(labelCountInfo.label, new AtomicLong(0));
                            edgePartitionCounts.get(partition).get(labelCountInfo.label).addAndGet(labelCountInfo.count);
                        }
                    }
                }

                // If shutdown signal have been asserted, exit the loop.
                //
                // We also check that the countdown has not been moved. Now this isn't a perfect system since we
                // actually re-assign the CountDownLatch before we empty the maps, so we can't say for sure we need
                // to loop again, but if the countdown is equal to the HIGH_WATERMARK, we can say for sure that we
                // don't need to loop again, therefore if we come around again we will exit properly.
                //
                // We add an or failed check because if Aerospike is seemingly not responding and firefly has been
                // signaled to shut down, we should just exit.
                if (SHUTDOWN.get() && (COUNTDOWN_LATCH.getCount() == HIGH_WATERMARK || failed)) {
                    // This is worst case scenario for timing of a shutdown signal and not having written data. Since the
                    // write failed, Aerospike could be down. Either way the loop needs to exit.
                    if (failed) {
                        LOG.warn("Failed to write all metadata to the summary vertex before taking poison pill. This may cause metadata skew.");
                    }

                    return false;
                }
            }
        } catch (final Exception e) {
            // A completely unexpected exception occurred here - keep the summary updater runner alive and return true.
            LOG.error("Error writing metadata to Aerospike.", e);
        }
        return true;
    }

    public static class FireflyElementMetadata {
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
                LOG.info("Vertex label: {}, count: {}, totalCount: {}", entry.getKey(), entry.getValue().count, totalVertexCount);
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
                LOG.info("Edge label: {}, count: {}, totalCount: {}", entry.getKey(), entry.getValue().count, totalEdgeCount);
            }
            return totalEdgeCount;
        }
    }

    public static class FireflyPropertiesAndCount {
        public final Set<String> properties;
        public long count;

        public FireflyPropertiesAndCount(final Set<String> properties, final long count) {
            this.properties = properties;
            this.count = count;
        }

        private void increment(final long count) {
            this.count += count;
        }
    }

    private Queue<KeyRecord> getPartitionRecords(final String set) {
        final Queue<KeyRecord> keyRecords = new ConcurrentLinkedQueue<>();
        final ScanPolicy scanPolicy = new ScanPolicy();
        scanPolicy.sendKey = true;
        db.scanAll(scanPolicy, set, (key, record) -> {
            // If the previous metadata was written without sendKey == true, this won't be populated.
            if (key.userKey == null) {
                return;
            }

            // If not a partition piece, skip.
            if (!key.userKey.toString().startsWith(VP_PROPERTY_PREFIX + "PART_") &&
                    !key.userKey.toString().startsWith(EP_PROPERTY_PREFIX + "PART_")) {
                return;
            }
            keyRecords.add(new KeyRecord(key, record));
        });
        return keyRecords;
    }

    private Map<String, Long> getEdgePartitionCounts(final Queue<KeyRecord> recordList) {
        final Map<String, Long> edgePartitionCounts = new HashMap<>();
        for (final KeyRecord keyRecord : recordList) {
            if (keyRecord.key.userKey.toString().startsWith(EP_PROPERTY_PREFIX + "PART_")) {
                if (keyRecord.record.bins.keySet().contains(SUMMARY_LABEL_BIN)) {
                    final Map<String, Long> edgeLabelCountMapkeyRecord = (Map) keyRecord.record.getMap(SUMMARY_LABEL_BIN);
                    for (final String label : edgeLabelCountMapkeyRecord.keySet()) {
                        edgePartitionCounts.putIfAbsent(label, 0L);
                        edgePartitionCounts.put(label, edgePartitionCounts.get(label) + edgeLabelCountMapkeyRecord.get(label));
                    }
                }
            }
        }
        return edgePartitionCounts;
    }

    private Map<String, Long> getVertexPartitionCounts(final Queue<KeyRecord> recordList) {
        final Map<String, Long> vertexPartitionCounts = new HashMap<>();
        for (final KeyRecord keyRecord : recordList) {
            if (keyRecord.key.userKey.toString().startsWith(VP_PROPERTY_PREFIX + "PART_")) {
                if (keyRecord.record.bins.keySet().contains(SUMMARY_LABEL_BIN)) {
                    final Map<String, Long> vertexLabelCountMap = (Map) keyRecord.record.getMap(SUMMARY_LABEL_BIN);
                    for (final String label : vertexLabelCountMap.keySet()) {
                        vertexPartitionCounts.putIfAbsent(label, 0L);
                        vertexPartitionCounts.put(label, vertexPartitionCounts.get(label) + vertexLabelCountMap.get(label));
                    }
                }
            }
        }
        return vertexPartitionCounts;
    }

    public FireflyElementMetadata getFireflyStatistics() {
        return getFireflyStatistics(false);
    }

    public FireflyElementMetadata getFireflyStatistics(final boolean isBulkLoaderRunning) {
        // Grab vertex metadata.
        final Record vertexLabelSummaryRecord = db.read(V_SUMMARY_KEY, null);
        final Record vertexPropertySummaryRecord = db.read(VP_SUMMARY_KEY, null);
        final Map<String, FireflyPropertiesAndCount> vertexMetadata = new HashMap<>();
        if ((vertexLabelSummaryRecord != null && vertexLabelSummaryRecord.bins.containsKey(SUMMARY_LABEL_BIN)) ||
                (vertexPropertySummaryRecord != null && vertexPropertySummaryRecord.bins.containsKey(SUMMARY_PROPERTY_BIN))) {
            Map<String, Long> vertexCountByLabel = new HashMap<>();
            if (vertexLabelSummaryRecord != null && vertexLabelSummaryRecord.bins.containsKey(SUMMARY_LABEL_BIN)) {
                vertexCountByLabel = (Map<String, Long>) vertexLabelSummaryRecord.bins.get(SUMMARY_LABEL_BIN);
            }
            Map<String, List<String>> vertexPropertiesByLabel = new HashMap<>();
            if (vertexPropertySummaryRecord != null && vertexPropertySummaryRecord.bins.containsKey(SUMMARY_PROPERTY_BIN)) {
                vertexPropertiesByLabel = (Map<String, List<String>>) vertexPropertySummaryRecord.bins.get(SUMMARY_PROPERTY_BIN);
            }
            final Set<String> vertexKeys = new HashSet<>();
            vertexKeys.addAll(vertexCountByLabel.keySet());
            vertexKeys.addAll(vertexPropertiesByLabel.keySet());
            for (final String vertexLabel : vertexKeys) {
                final long count = vertexCountByLabel.getOrDefault(vertexLabel, 0L);
                final Set<String> properties = new HashSet<>(vertexPropertiesByLabel.getOrDefault(vertexLabel, new ArrayList<>()));
                vertexMetadata.put(vertexLabel, new FireflyPropertiesAndCount(properties, count));
            }
            for (final String label : vertexPropertiesByLabel.keySet()) {
                vertexLabelToProperties.putIfAbsent(label, new HashSet<>());
                vertexLabelToProperties.get(label).addAll(vertexPropertiesByLabel.get(label));
            }
        }

        // Grab edge metadata.
        final Record edgeLabelSummaryRecord = db.read(E_SUMMARY_KEY, null);
        final Record edgePropertySummaryRecord = db.read(EP_SUMMARY_KEY, null);
        final Map<String, FireflyPropertiesAndCount> edgeMetadata = new HashMap<>();
        if ((edgeLabelSummaryRecord != null && edgeLabelSummaryRecord.bins.containsKey(SUMMARY_LABEL_BIN)) ||
                (edgePropertySummaryRecord != null && edgePropertySummaryRecord.bins.containsKey(SUMMARY_PROPERTY_BIN))) {
            final Map<String, Long> edgeCountByLabel = edgeLabelSummaryRecord == null ? new HashMap<>() : (Map) edgeLabelSummaryRecord.bins.get(SUMMARY_LABEL_BIN);
            final Map<String, List<String>> edgePropertiesByLabel = edgePropertySummaryRecord == null ? new HashMap<>() : (Map) edgePropertySummaryRecord.bins.get(SUMMARY_PROPERTY_BIN);
            final Set<String> edgeLabels = new HashSet<>();
            edgeLabels.addAll(edgeCountByLabel.keySet());
            edgeLabels.addAll(edgePropertiesByLabel.keySet());
            for (final String edgeLabel : edgeLabels) {
                final long count = edgeCountByLabel.getOrDefault(edgeLabel, 0L);
                final Set<String> properties = new HashSet<>(edgePropertiesByLabel.getOrDefault(edgeLabel, new ArrayList<>()));
                edgeMetadata.put(edgeLabel, new FireflyPropertiesAndCount(properties, count));
            }
            for (final String label : edgePropertiesByLabel.keySet()) {
                edgeLabelToProperties.putIfAbsent(label, new HashSet<>());
                edgeLabelToProperties.get(label).addAll(edgePropertiesByLabel.get(label));
            }
        }

        // If bulk loader is running, get staged partition counts and insert.
        if (isBulkLoaderRunning) {
            final Queue<KeyRecord> partitionRecords = getPartitionRecords(db.SUMMARY_SET);
            final Map<String, Long> vertexPartitionCounts = getVertexPartitionCounts(partitionRecords);
            final Map<String, Long> edgePartitionCounts = getEdgePartitionCounts(partitionRecords);
            for (final String label : vertexPartitionCounts.keySet()) {
                vertexMetadata.computeIfAbsent(label, k -> new FireflyPropertiesAndCount(Set.of(), 0));
                vertexMetadata.get(label).increment(vertexPartitionCounts.get(label));
            }
            for (final String label : edgePartitionCounts.keySet()) {
                edgeMetadata.computeIfAbsent(label, k -> new FireflyPropertiesAndCount(Set.of(), 0));
                edgeMetadata.get(label).increment(edgePartitionCounts.get(label));
            }
        }

        return new FireflyElementMetadata(vertexMetadata, edgeMetadata);
    }

    /**
     * Get the last exception that caused updating the summary ticker to fail. Meant for use only by the bulk loader.
     *
     * @param graphName name of the graph bulk loading is running on
     * @return the last AerospikeException that caused updating the ticker to fail or null
     */
    public static AerospikeGraphException getLastSummaryTickerException(final String graphName) {
        return LAST_SUMMARY_TICKER_EXCEPTION.get(graphName);
    }

    /**
     * Clean up the summary updater exception map after bulk loading. Meant for use only by the bulk loader.
     *
     * @param graphName name of the graph bulk loading was running on
     */
    public static void clearSummaryTickerException(final String graphName) {
        LAST_SUMMARY_TICKER_EXCEPTION.remove(graphName);
    }

    private void printGraphSummaryTicker() {
        if (!db.SUMMARY_TICKER_ENABLED_FLAG) {
            return;
        }

        final long TICKER_OUTPUT_INTERVAL_MS = 60000L;
        if (lastTickerOutputTime.get() + TICKER_OUTPUT_INTERVAL_MS > System.currentTimeMillis()) {
            return;
        }

        final FireflyElementMetadata fireflyElementMetadata = getFireflyStatistics();
        LOG.info("Graph summary ticker for " + db.GRAPH_ID + ":\n" + PRETTY_PRINT_FORMAT_LOG,
                fireflyElementMetadata.totalVertexCount(),
                fireflyElementMetadata.vertexCountByLabel(),
                fireflyElementMetadata.vertexPropertiesByLabel(),
                fireflyElementMetadata.totalEdgeCount(),
                fireflyElementMetadata.edgeCountByLabel(),
                fireflyElementMetadata.edgePropertiesByLabel());
        lastTickerOutputTime.set(System.currentTimeMillis());
    }

    public Runnable getUpdateRunnable() {
        return () -> {
            boolean hadError = false;
            while (true) {
                try {
                    printGraphSummaryTicker();
                } catch (final AerospikeGraphException e) {
                    // This should work but in case it doesn't, continue into standard operation.
                    if (this.db.getBulkLoaderFlag() && this.db.getOlapFlag()) {
                        // This is okay for now since this is a failing edge case and the code within is fast, but
                        // may need to add more complex logic to synchronize on unique graph names in the future.
                        synchronized (LAST_SUMMARY_TICKER_EXCEPTION) {
                            final AerospikeGraphException lastException = LAST_SUMMARY_TICKER_EXCEPTION.get(this.db.GRAPH_ID);
                            if (lastException == null || lastException.errorCode != e.errorCode) {
                                LAST_SUMMARY_TICKER_EXCEPTION.put(this.db.GRAPH_ID, e);
                                LOG.warn("Failed to print graph summary ticker.", e);
                            }
                        }
                    } else {
                        LOG.warn("Failed to print graph summary ticker.", e);
                    }
                } catch (final RuntimeException e) {
                    LOG.warn("Failed to print graph summary ticker.", e);
                }

                try {
                    // If shutdown is initiated, go through the map and then exit (i.e skip CountDownLatch).
                    if (!SHUTDOWN.get()) {
                        // Wait for the countdown latch to complete.
                        // We don't really care whether it completed because of a timeout or because the watermark
                        // was hit, either way we should continue on and check what's in our maps.
                        COUNTDOWN_LATCH.await(LATCH_BREAK_TIME_MILLISECONDS, TimeUnit.MILLISECONDS);
                    }
                } catch (final InterruptedException e) {
                    // countDownLatch.await() should break before the interrupted exception comes in, therefore this is unexpected.
                    if (!hadError) {
                        hadError = true;
                        LOG.error("Error while updating summary vertex", e);
                    }
                    continue;
                }
                if (!doWrite()) {
                    break;
                }
            }
            EXITED.set(true);

            // Unblock the calling task.
            SHUTDOWN_LATCH.countDown();
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
        writePolicy.sendKey = true;

        // Write the updates to the summary record.
        for (final LabelCountInfo countInfo : updates) {
            // If we are truncating, we can just exit out and let it happen.
            if (TRUNCATION.get()) {
                break;
            }

            // Count can be 0 if we are adding a property to an existing vertex/edge.
            if (countInfo.count == 0) {
                continue;
            }

            final MapPolicy createOnlyPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.CREATE_ONLY | MapWriteFlags.NO_FAIL);
            final MapPolicy updateOnlyPolicy = new MapPolicy(MapOrder.KEY_ORDERED, MapWriteFlags.UPDATE_ONLY | MapWriteFlags.NO_FAIL);
            final Operation createOp = MapOperation.put(createOnlyPolicy, SUMMARY_LABEL_BIN, Value.get(countInfo.label), Value.get(0));
            final Operation updateOp = MapOperation.increment(updateOnlyPolicy, SUMMARY_LABEL_BIN, Value.get(countInfo.label), Value.get(countInfo.count));
            db.writeOperate(writePolicy, key, createOp, updateOp);

            // Remove count so that if one in the loop fails and we re-add these values to the map, they aren't all
            // added erroneously.
            countInfo.count = 0;
        }
    }

    private void writeLabelPropertiesOperations(final Set<LabelCountInfo> updates, final Map<String, Set<String>> propertyMappings, final Key key) {
        // Standard write policy.
        final WritePolicy writePolicy = new WritePolicy();
        writePolicy.sendKey = true;

        // Write the updates to the summary record.
        for (final LabelCountInfo updateInfo : updates) {
            for (final String property : updateInfo.properties) {
                // If we are truncating, we can just exit out and let it happen.
                if (TRUNCATION.get()) {
                    break;
                }

                // Check if we have already inserted this mapping.
                synchronized (FireflyGraphSummaryUpdater.class) {
                    if (propertyMappings.containsKey(updateInfo.label) &&
                            propertyMappings.get(updateInfo.label).contains(property)) {
                        // Skip it.
                        continue;
                    }
                }

                final ListPolicy createListOnlyPolicy = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL);
                final Operation createOp = ListOperation.create(SUMMARY_PROPERTY_BIN, ListOrder.UNORDERED, false, CTX.mapKey(Value.get(updateInfo.label)));
                final Operation updateOp = ListOperation.append(createListOnlyPolicy, SUMMARY_PROPERTY_BIN, Value.get(property), CTX.mapKey(Value.get(updateInfo.label)));
                db.writeOperate(writePolicy, key, createOp, updateOp);

                synchronized (FireflyGraphSummaryUpdater.class) {
                    if (!propertyMappings.containsKey(updateInfo.label)) {
                        propertyMappings.put(updateInfo.label, new HashSet<>());
                    }
                    propertyMappings.get(updateInfo.label).add(property);
                }
            }
        }
    }

    public void clearVertexPartitionData() {
        LOG.info("Clearing vertex partition data");
        final Queue<KeyRecord> partitionRecords = getPartitionRecords(db.SUMMARY_SET);
        for (final KeyRecord keyRecord : partitionRecords) {
            if (keyRecord.key.userKey.toString().startsWith(VP_PROPERTY_PREFIX + "PART_"))
                db.delete(keyRecord.key, null);
        }
    }

    public void clearEdgePartitionData() {
        LOG.info("Clearing edge partition data");
        final Queue<KeyRecord> partitionRecords = getPartitionRecords(db.SUMMARY_SET);
        for (final KeyRecord keyRecord : partitionRecords) {
            if (keyRecord.key.userKey.toString().startsWith(EP_PROPERTY_PREFIX + "PART_"))
                db.delete(keyRecord.key, null);
        }
    }
}
