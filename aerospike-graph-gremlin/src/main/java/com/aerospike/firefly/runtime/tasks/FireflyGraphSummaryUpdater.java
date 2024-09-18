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
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
            TRUNCATION.set(false);
        }
    }

    /**
     * Update the number of vertices or edges that exist under the provided label in the summary record.
     *
     * @param label      The label of the vertex or edge to update.
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
     * Update the number of vertices or edges that exist under the provided label in the summary record.
     *
     * @param label      The label of the vertex or edge to update.
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
     * Update the number of vertices or edges that exist under the provided label in the summary record.
     *
     * @param label      The label of the vertex or edge to update.
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

    private void printGraphSummaryTicker() {
        if (!db.SUMMARY_TICKER_ENABLED_FLAG) {
            return;
        }

        final long TICKER_OUTPUT_INTERVAL_MS = 60000L;
        if (lastTickerOutputTime.get() + TICKER_OUTPUT_INTERVAL_MS > System.currentTimeMillis()) {
            return;
        }

        final FireflyElementMetadata fireflyElementMetadata = getFireflyStatistics();
        LOG.info("Graph summary ticker:\n" + PRETTY_PRINT_FORMAT_LOG,
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
                } catch (final RuntimeException e) {
                    // This should work but in case it doesn't, continue into standard operation.
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
                    for (final String key: vertexCountsToUse.keySet()) {
                        final long count = vertexCountsToUse.get(key).getAndSet(0);
                        final Set<String> properties = vertexPropertiesToUse.get(key);
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

                    for (final String key: edgeCountsToUse.keySet()) {
                        final long count = edgeCountsToUse.get(key).getAndSet(0);
                        final Set<String> properties = edgePropertiesToUse.get(key);
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
                        break;
                    }
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
}
