/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.runtime.tasks;

import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Txn;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.cdt.MapOperation;
import com.aerospike.client.cdt.MapOrder;
import com.aerospike.client.cdt.MapPolicy;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.cdt.MapWriteFlags;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.ExpOperation;
import com.aerospike.client.exp.ExpWriteFlags;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
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
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static com.aerospike.firefly.io.aerospike.AerospikeConnection.getDefaultThreadPoolSize;
import static com.aerospike.firefly.process.call.metadata.MetadataServiceSummary.PRETTY_PRINT_FORMAT_LOG;

public class FireflyGraphSummaryUpdater implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyGraphSummaryUpdater.class);
    private static Integer MAX_CONCURRENT_TXN_COUNT = null;
    private static final int HIGH_WATERMARK = 250;
    public static final int MAP_RECYCLE_SIZE = 5000;
    private static final int LATCH_BREAK_TIME_MILLISECONDS = 1000;
    private static final String V_SUMMARY_RECORD = "~V_SUMMARY";
    private static final String E_SUMMARY_RECORD = "~E_SUMMARY";
    private static final String S_SUMMARY_RECORD = "~S_SUMMARY";
    private static final String VP_PROPERTY_PREFIX = "~VP_";
    private static final String EP_PROPERTY_PREFIX = "~EP_";
    private static final String SP_PROPERTY_PREFIX = "~SP_";
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
    public Map<String, AtomicLong> supernodeCounts = new ConcurrentHashMap<>();
    public final Map<Integer, Map<String, AtomicLong>> vertexPartitionCounts = new ConcurrentHashMap<>();
    public final Map<Integer, Map<String, AtomicLong>> mergeVertexPartitionCounts = new ConcurrentHashMap<>();
    public final Map<Integer, Map<String, AtomicLong>> edgePartitionCounts = new ConcurrentHashMap<>();
    public final Map<Integer, Map<String, AtomicLong>> supernodePartitionCounts = new ConcurrentHashMap<>();

    // MRT and Tx summary info
    public final Map<Long, Map<String, Set<String>>> txnEdgeProperties = new ConcurrentHashMap<>();
    public final Map<Long, Map<String, Set<String>>> txnVertexProperties = new ConcurrentHashMap<>();
    public final Map<Long, Map<String, AtomicLong>> txnEdgeCounts = new ConcurrentHashMap<>();
    public final Map<Long, Map<String, AtomicLong>> txnVertexCounts = new ConcurrentHashMap<>();
    public final Map<Long, Map<String, AtomicLong>> txnSupernodeCounts = new ConcurrentHashMap<>();

    private final Key V_SUMMARY_KEY;
    private final Key E_SUMMARY_KEY;
    private final Key S_SUMMARY_KEY;
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
        if (MAX_CONCURRENT_TXN_COUNT == null) {
            MAX_CONCURRENT_TXN_COUNT = getDefaultThreadPoolSize(FireflyGraph.getGremlinServerSettings());
        }
        this.db = db;
        this.VP_SUMMARY_KEY = new Key(db.getNamespace(), db.getConfig().summarySet, VP_PROPERTY_PREFIX + SUMMARY_PROPERTY_BIN);
        this.EP_SUMMARY_KEY = new Key(db.getNamespace(), db.getConfig().summarySet, EP_PROPERTY_PREFIX + SUMMARY_PROPERTY_BIN);
        this.V_SUMMARY_KEY = new Key(db.getNamespace(), db.getConfig().summarySet, V_SUMMARY_RECORD);
        this.E_SUMMARY_KEY = new Key(db.getNamespace(), db.getConfig().summarySet, E_SUMMARY_RECORD);
        this.S_SUMMARY_KEY = new Key(db.getNamespace(), db.getConfig().summarySet, S_SUMMARY_RECORD);
        if (db.getConfig().summaryEnabledFlag) {
            synchronized (EXECUTOR_SERVICE) {
                if (RUNNING_COUNT.addAndGet(1) == 1) {
                    EXITED.set(false);
                    SHUTDOWN.set(false);
                    EXECUTOR_SERVICE.submit(getUpdateRunnable());
                }
                AerospikeConnection.InfoOps.createSetIndex(db, db.getConfig().summarySet);
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
            supernodeCounts.clear();
            vertexPartitionCounts.clear();
            edgePartitionCounts.clear();
            supernodePartitionCounts.clear();
            mergeVertexPartitionCounts.clear();
            TRUNCATION.set(false);
        }
    }

    /**
     * Update the number of vertices that exist under the provided label in the summary record.
     *
     * @param label      The label of the vertex to update.
     * @param properties The properties of the vertex to update.
     * @param txn        The transaction that this operation occurred within.
     */
    public void addVertexWriteToQueue(final String label, final Set<String> properties, final Txn txn) {
        if (!db.getConfig().summaryEnabledFlag) {
            return;
        }

        final Long id = txn == null ? null : txn.getId();

        final Map<String, AtomicLong> counts = (id == null) ? vertexCounts :
                txnVertexCounts.computeIfAbsent(id, k -> new ConcurrentHashMap<>());
        counts.computeIfAbsent(label, k -> new AtomicLong(0));
        counts.get(label).addAndGet(1);
        final Map<String, Set<String>> propertiesMap = (id == null) ? vertexProperties :
                txnVertexProperties.computeIfAbsent(id, k -> new ConcurrentHashMap<>());
        propertiesMap.computeIfAbsent(label, k -> ConcurrentHashMap.newKeySet());
        propertiesMap.get(label).addAll(properties);

        if (txn == null) {
            COUNTDOWN_LATCH.countDown();
        }
    }

    /**
     * Stage an update to the number of vertices that exist under the provided label in the summary record.
     *
     * @param label      The label of the vertex to update.
     * @param properties The properties of the vertex to update.
     */
    public void stageVertexWriteToQueue(final String label, final Set<String> properties, final int partitionId) {
        if (!db.getConfig().summaryEnabledFlag) {
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
     * Stage an update to the number of vertices that exist under the provided label in the summary record.
     *
     * @param label      The label of the vertex to update.
     */
    public void stageVertexMergeToQueue(final String label, final int partitionId) {
        if (!db.getConfig().summaryEnabledFlag) {
            return;
        }

        mergeVertexPartitionCounts.computeIfAbsent(partitionId, k -> new ConcurrentHashMap<>());
        mergeVertexPartitionCounts.get(partitionId).computeIfAbsent(label, k -> new AtomicLong(0));
        mergeVertexPartitionCounts.get(partitionId).get(label).addAndGet(1);
        COUNTDOWN_LATCH.countDown();
    }

    /**
     * Update the number of vertices that exist under the provided label in the summary record.
     *
     * @param label The label of the vertex to remove.
     * @param txn   The transaction that this operation occurred within.
     */
    public void addVertexRemoveToQueue(final String label, final Txn txn) {
        if (!db.getConfig().summaryEnabledFlag) {
            return;
        }

        final Long id = txn == null ? null : txn.getId();

        final Map<String, AtomicLong> counts = (id == null) ? vertexCounts :
                txnVertexCounts.computeIfAbsent(id, k -> new ConcurrentHashMap<>());
        counts.computeIfAbsent(label, k -> new AtomicLong(0));
        counts.get(label).addAndGet(-1);

        if (txn == null) {
            COUNTDOWN_LATCH.countDown();
        }
    }

    /**
     * Update the number of edges that exist under the provided label in the summary record.
     *
     * @param label         The label of the edge to update.
     * @param properties    The properties of the edge to update.
     * @param txn           The transaction that this operation occurred within.
     */
    public void addEdgeWriteToQueue(final String label, final Set<String> properties, final Txn txn) {
        if (!db.getConfig().summaryEnabledFlag) {
            return;
        }

        final Long id = txn == null ? null : txn.getId();

        final Map<String, AtomicLong> counts = (id == null) ? edgeCounts :
                txnEdgeCounts.computeIfAbsent(id, k -> new ConcurrentHashMap<>());
        counts.computeIfAbsent(label, k -> new AtomicLong(0));
        counts.get(label).addAndGet(1);
        final Map<String, Set<String>> propertiesMap = (id == null) ? edgeProperties :
                txnEdgeProperties.computeIfAbsent(id, k -> new ConcurrentHashMap<>());
        propertiesMap.computeIfAbsent(label, k -> ConcurrentHashMap.newKeySet());
        propertiesMap.get(label).addAll(properties);

        if (txn == null) {
            COUNTDOWN_LATCH.countDown();
        }
    }

    /**
     * Stage an update the number of edges that exist under the provided label in the summary record.
     *
     * @param label      The label of the edge to update.
     * @param properties The properties of the edge to update.
     */
    public void stageEdgeWriteToQueue(final String label, final Set<String> properties, final int partitionId) {
        if (!db.getConfig().summaryEnabledFlag) {
            return;
        }

        edgePartitionCounts.computeIfAbsent(partitionId, k -> new ConcurrentHashMap<>());
        edgePartitionCounts.get(partitionId).computeIfAbsent(label, k -> new AtomicLong(0));
        edgePartitionCounts.get(partitionId).get(label).addAndGet(1);
        edgeProperties.computeIfAbsent(label, k -> ConcurrentHashMap.newKeySet());
        edgeProperties.get(label).addAll(properties);
        COUNTDOWN_LATCH.countDown();
    }

    /**
     * Update the number of supernodes that exist under the provided label in the summary record.
     *
     * @param label The label of the supernode to update.
     * @param txn   The transaction that this operation occurred within.
     */
    public void addSupernodeWriteToQueue(final String label, final Txn txn) {
        if (!db.getConfig().summaryEnabledFlag) {
            return;
        }

        final Long id = txn == null ? null : txn.getId();

        final Map<String, AtomicLong> counts = (id == null) ? supernodeCounts :
                txnSupernodeCounts.computeIfAbsent(id, k -> new ConcurrentHashMap<>());
        counts.computeIfAbsent(label, k -> new AtomicLong(0));
        counts.get(label).addAndGet(1);

        if (txn == null) {
            COUNTDOWN_LATCH.countDown();
        }
    }

    /**
     * Stage an update to the number of supernodes that exist under the provided label in the summary record.
     *
     * @param label The label of the supernode to update.
     */
    public void stageSupernodeWriteToQueue(final String label, final int partitionId) {
        if (!db.getConfig().summaryEnabledFlag) {
            return;
        }

        supernodePartitionCounts.computeIfAbsent(partitionId, k -> new ConcurrentHashMap<>());
        supernodePartitionCounts.get(partitionId).computeIfAbsent(label, k -> new AtomicLong(0));
        supernodePartitionCounts.get(partitionId).get(label).addAndGet(1);
        COUNTDOWN_LATCH.countDown();
    }

    /**
     * Update the number of supernodes that exist under the provided label in the summary record.
     *
     * @param label The label of the supernode to remove.
     * @param txn   The transaction that this operation occurred within.
     */
    public void addSupernodeRemoveToQueue(final String label, final Txn txn) {
        if (!db.getConfig().summaryEnabledFlag) {
            return;
        }

        final Long id = txn == null ? null : txn.getId();

        final Map<String, AtomicLong> counts = (id == null) ? supernodeCounts :
                txnSupernodeCounts.computeIfAbsent(id, k -> new ConcurrentHashMap<>());
        counts.computeIfAbsent(label, k -> new AtomicLong(0));
        counts.get(label).addAndGet(-1);

        if (txn == null) {
            COUNTDOWN_LATCH.countDown();
        }
    }

    public void startVertexPartition(final int partitionId) {
        if (!db.getConfig().summaryEnabledFlag) {
            return;
        }

        // Need to kill what is in Aerospike because this is from a failed partition if it exists.
        final Key vertexPartitionKey = getVertexPartitionKey(partitionId);
        db.delete(vertexPartitionKey, null);
    }

    public void startMergeVertexPartition(final int partitionId) {
        if (!db.getConfig().summaryEnabledFlag) {
            return;
        }

        // Need to kill what is in Aerospike because this is from a failed partition if it exists.
        final Key vertexPartitionKey = getVertexPartitionKey(partitionId);
        db.delete(vertexPartitionKey, null);
    }

    public void completeVertexPartition(final int partitionId) {
        if (!db.getConfig().summaryEnabledFlag) {
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
        db.delete(vertexPartitionKey, null);
        forceWrite();
    }

    public void startEdgePartition(final int partitionId) {
        if (!db.getConfig().summaryEnabledFlag) {
            return;
        }
        // Need to kill what is in Aerospike because this is from a failed partition if it exists.
        final Key edgePartitionKey = getEdgePartitionKey(partitionId);
        db.delete(edgePartitionKey, null);
    }

    public void completeEdgePartition(final int partitionId) {
        if (!db.getConfig().summaryEnabledFlag) {
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
        db.delete(edgePartitionKey, null);
        forceWrite();
    }

    public void startSupernodePartition(final int partitionId) {
        if (!db.getConfig().summaryEnabledFlag) {
            return;
        }

        // Need to kill what is in Aerospike because this is from a failed partition if it exists.
        final Key supernodePartitionKey = getSupernodePartitionKey(partitionId);
        db.delete(supernodePartitionKey, null);
    }

    public void completeSupernodePartition(final int partitionId) {
        if (!db.getConfig().summaryEnabledFlag) {
            return;
        }

        // Need to get what is in Aerospike for this partition and move it to the main dataset.
        final Key supernodePartitionKey = getSupernodePartitionKey(partitionId);
        final Map<String, Long> labelToCount = getPartitionCounts(supernodePartitionKey);
        if (supernodePartitionCounts.containsKey(partitionId)) {
            for (final String label : supernodePartitionCounts.get(partitionId).keySet()) {
                supernodeCounts.computeIfAbsent(label, k -> new AtomicLong(0));
                supernodeCounts.get(label).addAndGet(supernodePartitionCounts.get(partitionId).get(label).get());
            }
        }
        for (final String label : labelToCount.keySet()) {
            supernodeCounts.computeIfAbsent(label, k -> new AtomicLong(0));
            supernodeCounts.get(label).addAndGet(labelToCount.get(label));
        }
        db.delete(supernodePartitionKey, null);
        forceWrite();
    }

    private Key getEdgePartitionKey(final int partitionId) {
        return new Key(db.getNamespace(), db.getConfig().summarySet, EP_PROPERTY_PREFIX + "PART_" + partitionId);
    }

    private Key getVertexPartitionKey(final int partitionId) {
        return new Key(db.getNamespace(), db.getConfig().summarySet, VP_PROPERTY_PREFIX + "PART_" + partitionId);
    }

    private Key getMergeVertexPartitionKey(final int partitionId) {
        return new Key(db.getNamespace(), db.getConfig().summarySet, VP_PROPERTY_PREFIX + "MERGEV_" + partitionId);
    }

    private Key getSupernodePartitionKey(final int partitionId) {
        return new Key(db.getNamespace(), db.getConfig().summarySet, SP_PROPERTY_PREFIX + "PART_" + partitionId);
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
     * @param label The label of the vertex or edge to update.
     */
    public void addEdgeRemoveToQueue(final String label, final Txn txn) {
        if (!db.getConfig().summaryEnabledFlag) {
            return;
        }

        final Long id = txn == null ? null : txn.getId();

        final Map<String, AtomicLong> counts = (id == null) ? edgeCounts :
                txnEdgeCounts.computeIfAbsent(id, k -> new ConcurrentHashMap<>());
        counts.computeIfAbsent(label, k -> new AtomicLong(0));
        counts.get(label).addAndGet(-1);

        if (txn == null) {
            COUNTDOWN_LATCH.countDown();
        }
    }

    /**
     * Update the properties of an existing vertex (therefore do not update count).
     *
     * @param label      The label of the vertex or edge to update.
     * @param properties The properties of the vertex to update.
     */
    public void addVertexPropertiesWriteToQueue(final String label, final Set<String> properties, final Txn txn) {
        if (!db.getConfig().summaryEnabledFlag) {
            return;
        }

        final Long id = txn == null ? null : txn.getId();

        final Map<String, Set<String>> propertiesMap = (id == null) ? vertexProperties :
                txnVertexProperties.computeIfAbsent(id, k -> new ConcurrentHashMap<>());
        propertiesMap.computeIfAbsent(label, k -> ConcurrentHashMap.newKeySet());
        propertiesMap.get(label).addAll(properties);

        if (txn == null) {
            COUNTDOWN_LATCH.countDown();
        }
    }

    /**
     * Update the properties of an existing edge (therefore do not update count).
     *
     * @param label      The label of the edge to update.
     * @param properties The properties of the edge to update.
     */
    public void addEdgePropertiesWriteToQueue(final String label, final Set<String> properties, final Txn txn) {
        if (!db.getConfig().summaryEnabledFlag) {
            return;
        }

        final Long id = txn == null ? null : txn.getId();

        final Map<String, Set<String>> propertiesMap = (id == null) ? edgeProperties :
                txnEdgeProperties.computeIfAbsent(id, k -> new ConcurrentHashMap<>());
        propertiesMap.computeIfAbsent(label, k -> ConcurrentHashMap.newKeySet());
        propertiesMap.get(label).addAll(properties);

        if (txn == null) {
            COUNTDOWN_LATCH.countDown();
        }
    }

    public void commitSummaryForTxn(final Txn txn) {
        final long id = txn.getId();
        final Map<String, Set<String>> edgePropertiesForTxn = txnEdgeProperties.remove(id);
        final Map<String, Set<String>> vertexPropertiesForTxn = txnVertexProperties.remove(id);
        final Map<String, AtomicLong> edgeCountsForTxn = txnEdgeCounts.remove(id);
        final Map<String, AtomicLong> vertexCountsForTxn = txnVertexCounts.remove(id);
        final Map<String, AtomicLong> supernodeCountsForTxn = txnSupernodeCounts.remove(id);
        joinTxnSummaryDataSetToUpdater(edgePropertiesForTxn, edgeProperties);
        joinTxnSummaryDataSetToUpdater(vertexPropertiesForTxn, vertexProperties);
        joinTxnSummaryDataCounterToUpdater(edgeCountsForTxn, edgeCounts);
        joinTxnSummaryDataCounterToUpdater(vertexCountsForTxn, vertexCounts);
        joinTxnSummaryDataCounterToUpdater(supernodeCountsForTxn, supernodeCounts);

        long totalCount = 0;
        if (edgeCountsForTxn != null) {
            for (final AtomicLong count : edgeCountsForTxn.values()) {
                totalCount += count.get();
            }
        }
        if (vertexCountsForTxn != null) {
            for (final AtomicLong count : vertexCountsForTxn.values()) {
                totalCount += count.get();
            }
        }
        countDownLatchBy(totalCount);
    }

    public void abortSummaryForTxn(final Txn txn) {
        final long id = txn.getId();
        txnEdgeProperties.remove(id);
        txnVertexProperties.remove(id);
        txnEdgeCounts.remove(id);
        txnVertexCounts.remove(id);
        txnSupernodeCounts.remove(id);
    }

    private void joinTxnSummaryDataSetToUpdater(final Map<String, Set<String>> txnMap, final Map<String, Set<String>> graphMap) {
        if (txnMap == null) {
            return;
        }

        for (final Map.Entry<String, Set<String>> txnSetKv : txnMap.entrySet()) {
            if (graphMap.containsKey(txnSetKv.getKey())) {
                graphMap.get(txnSetKv.getKey()).addAll(txnSetKv.getValue());
            } else {
                graphMap.put(txnSetKv.getKey(), txnSetKv.getValue());
            }
        }
    }

    private void joinTxnSummaryDataCounterToUpdater(final Map<String, AtomicLong> txnMap, final Map<String, AtomicLong> graphMap) {
        if (txnMap == null) {
            return;
        }

        for (final Map.Entry<String, AtomicLong> txnSetKv : txnMap.entrySet()) {
            if (graphMap.containsKey(txnSetKv.getKey())) {
                graphMap.get(txnSetKv.getKey()).addAndGet(txnSetKv.getValue().get());
            } else {
                graphMap.put(txnSetKv.getKey(), txnSetKv.getValue());
            }
        }
    }

    @Override
    public void close() {
        if (!db.getConfig().summaryEnabledFlag) {
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
        forceWrite();

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

        if (!vertexCounts.isEmpty() || !edgeCounts.isEmpty() || !supernodeCounts.isEmpty())
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
                // Sanity check for Txn summary leaks.
               if (this.txnEdgeProperties.size() > MAX_CONCURRENT_TXN_COUNT ||
                       this.txnVertexProperties.size() > MAX_CONCURRENT_TXN_COUNT ||
                       this.txnEdgeCounts.size() > MAX_CONCURRENT_TXN_COUNT ||
                       this.txnVertexCounts.size() > MAX_CONCURRENT_TXN_COUNT ||
                       this.txnSupernodeCounts.size() > MAX_CONCURRENT_TXN_COUNT) {
                   LOG.error("AGS Summary unexpectedly detected more open parallel transactions than permissible. " +
                           "Summary info may become slightly inaccurate as current uncommitted data is reset. " +
                           "Please contact support and report this error.");
                   txnEdgeProperties.clear();
                   txnVertexProperties.clear();
                   txnEdgeCounts.clear();
                   txnVertexCounts.clear();
                   txnSupernodeCounts.clear();
               }

                // Take the data from update info into a map.
                final Map<String, LabelCountInfo> vertexUpdates = new HashMap<>();
                final Map<String, LabelCountInfo> edgeUpdates = new HashMap<>();
                final Map<String, LabelCountInfo> supernodeUpdates = new HashMap<>();

                // At this point we are about to take all the data out of the maps and put them into a new map.
                // We did not synchronize this, but we don't care about exacts, only roughly how much is coming in,
                // so now is an appropriate time to reset the latch.
                COUNTDOWN_LATCH = new CountDownLatch(HIGH_WATERMARK);

                final Map<String, AtomicLong> vertexCountsToUse = vertexCounts;
                final Map<String, Set<String>> vertexPropertiesToUse = vertexProperties;
                if (vertexCounts.size() > MAP_RECYCLE_SIZE || vertexProperties.size() > MAP_RECYCLE_SIZE) {
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
                if (edgeCounts.size() > MAP_RECYCLE_SIZE || edgeProperties.size() > MAP_RECYCLE_SIZE) {
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

                final Map<String, AtomicLong> supernodeCountsToUse = supernodeCounts;
                if (supernodeCounts.size() > MAP_RECYCLE_SIZE) {
                    // Ideally we don't want to have to do this, but we don't want the map to grow infinitely in the case
                    // that firefly never gets shut down and the customer for some odd reason keeps using new labels.
                    supernodeCounts = new ConcurrentHashMap<>();
                }

                final Set<String> supernodeKeys = new HashSet<>(supernodeCountsToUse.keySet());
                for (final String key : supernodeKeys) {
                    supernodeCountsToUse.computeIfAbsent(key, k -> new AtomicLong(0));
                    final long count = supernodeCountsToUse.get(key).getAndSet(0);
                    supernodeUpdates.put(key, new LabelCountInfo(key, count, new HashSet<>()));
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

                if (didFunctionFail(() -> writeLabelCountOperations(new HashSet<>(supernodeUpdates.values()), S_SUMMARY_KEY))) {
                    failed = true;
                    for (final LabelCountInfo labelCountInfo : supernodeUpdates.values()) {
                        supernodeCounts.putIfAbsent(labelCountInfo.label, new AtomicLong(0));
                        supernodeCounts.get(labelCountInfo.label).addAndGet(labelCountInfo.count);
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

                for (final Integer partition : mergeVertexPartitionCounts.keySet()) {
                    final Map<String, LabelCountInfo> partitionMergeVertexUpdates = new HashMap<>();
                    final Map<String, AtomicLong> partitionMergeVertex = mergeVertexPartitionCounts.getOrDefault(partition, new ConcurrentHashMap<>());
                    for (final String key : partitionMergeVertex.keySet()) {
                        final long count = partitionMergeVertex.get(key).getAndSet(0);
                        partitionMergeVertexUpdates.put(key, new LabelCountInfo(key, count, new HashSet<>()));
                    }

                    if (didFunctionFail(() -> writeLabelCountOperations(new HashSet<>(partitionMergeVertexUpdates.values()), getMergeVertexPartitionKey(partition)))) {
                        failed = true;
                        for (final LabelCountInfo labelCountInfo : partitionMergeVertexUpdates.values()) {
                            mergeVertexPartitionCounts.putIfAbsent(partition, new ConcurrentHashMap<>());
                            mergeVertexPartitionCounts.get(partition).putIfAbsent(labelCountInfo.label, new AtomicLong(0));
                            mergeVertexPartitionCounts.get(partition).get(labelCountInfo.label).addAndGet(labelCountInfo.count);
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

                for (final Integer partition : supernodePartitionCounts.keySet()) {
                    final Map<String, LabelCountInfo> partitionSupernodeUpdates = new HashMap<>();
                    final Map<String, AtomicLong> partitionSupernodeCounts = supernodePartitionCounts.getOrDefault(partition, new ConcurrentHashMap<>());
                    for (final String key : partitionSupernodeCounts.keySet()) {
                        final long count = partitionSupernodeCounts.get(key).getAndSet(0);
                        partitionSupernodeUpdates.put(key, new LabelCountInfo(key, count, new HashSet<>()));
                    }

                    if (didFunctionFail(() -> writeLabelCountOperations(new HashSet<>(partitionSupernodeUpdates.values()), getSupernodePartitionKey(partition)))) {
                        failed = true;
                        for (final LabelCountInfo labelCountInfo : partitionSupernodeUpdates.values()) {
                            supernodePartitionCounts.putIfAbsent(partition, new ConcurrentHashMap<>());
                            supernodePartitionCounts.get(partition).putIfAbsent(labelCountInfo.label, new AtomicLong(0));
                            supernodePartitionCounts.get(partition).get(labelCountInfo.label).addAndGet(labelCountInfo.count);
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
        public final Map<String, FireflyPropertiesAndCount> supernodeInfo;
        public final Map<String, FireflyPropertiesAndCount> mergeVertexInfo = new HashMap<>();

        public FireflyElementMetadata(final Map<String, FireflyPropertiesAndCount> vertexInfo,
                                      final Map<String, FireflyPropertiesAndCount> edgeInfo,
                                      final Map<String, FireflyPropertiesAndCount> supernodeInfo,
                                      final Optional<Map<String, FireflyPropertiesAndCount>> mergeVertexInfo) {
            this.vertexInfo = vertexInfo;
            this.edgeInfo = edgeInfo;
            this.supernodeInfo = supernodeInfo;
            mergeVertexInfo.ifPresent(this.mergeVertexInfo::putAll);
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
         * Get the number of supernodes with the provided label.
         *
         * @return The number of supernodes with the provided label.
         */
        public Map<String, Long> supernodesCountByLabel() {
            final Map<String, Long> supernodesCountByLabel = new HashMap<>();
            for (final Map.Entry<String, FireflyPropertiesAndCount> entry : supernodeInfo.entrySet()) {
                supernodesCountByLabel.put(entry.getKey(), entry.getValue().count);
            }
            return supernodesCountByLabel;
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

        public long totalMergeVertexCount() {
            long totalMergeVertexCount = 0;
            for (final Map.Entry<String, FireflyPropertiesAndCount> entry : mergeVertexInfo.entrySet()) {
                totalMergeVertexCount += entry.getValue().count;
            }
            return totalMergeVertexCount;
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

        /**
         * Get total supernode count.
         *
         * @return The total supernode count.
         */
        public long totalSupernodeCount() {
            long totalSupernodeCount = 0;
            for (final Map.Entry<String, FireflyPropertiesAndCount> entry : supernodeInfo.entrySet()) {
                totalSupernodeCount += entry.getValue().count;
            }
            return totalSupernodeCount;
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
                    !key.userKey.toString().startsWith(EP_PROPERTY_PREFIX + "PART_") &&
                    !key.userKey.toString().startsWith(SP_PROPERTY_PREFIX + "PART_") &&
                    !key.userKey.toString().startsWith(VP_PROPERTY_PREFIX + "MERGEV_")) {
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
                if (keyRecord.record.bins.containsKey(SUMMARY_LABEL_BIN)) {
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
                if (keyRecord.record.bins.containsKey(SUMMARY_LABEL_BIN)) {
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

    private Map<String, Long> getMergeVertexPartitionCounts(final Queue<KeyRecord> recordList) {
        final Map<String, Long> vertexPartitionCounts = new HashMap<>();
        for (final KeyRecord keyRecord : recordList) {
            if (keyRecord.key.userKey.toString().startsWith(VP_PROPERTY_PREFIX + "MERGEV_")) {
                if (keyRecord.record.bins.containsKey(SUMMARY_LABEL_BIN)) {
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

    private Map<String, Long> getSupernodePartitionCounts(final Queue<KeyRecord> recordList) {
        final Map<String, Long> supernodePartitionCounts = new HashMap<>();
        for (final KeyRecord keyRecord : recordList) {
            if (keyRecord.key.userKey.toString().startsWith(SP_PROPERTY_PREFIX + "PART_")) {
                if (keyRecord.record.bins.containsKey(SUMMARY_LABEL_BIN)) {
                    final Map<String, Long> supernodeLabelCountMap = (Map) keyRecord.record.getMap(SUMMARY_LABEL_BIN);
                    for (final String label : supernodeLabelCountMap.keySet()) {
                        supernodePartitionCounts.putIfAbsent(label, 0L);
                        supernodePartitionCounts.put(label, supernodePartitionCounts.get(label) + supernodeLabelCountMap.get(label));
                    }
                }
            }
        }
        return supernodePartitionCounts;
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

        // Grab supernode metadata.
        final Record supernodeLabelSummaryRecord = db.read(S_SUMMARY_KEY, null);
        final Map<String, FireflyPropertiesAndCount> supernodeMetadata = new HashMap<>();
        if (supernodeLabelSummaryRecord != null && supernodeLabelSummaryRecord.bins.containsKey(SUMMARY_LABEL_BIN)) {
            final Map<String, Long> supernodeCountByLabel = (Map) supernodeLabelSummaryRecord.bins.get(SUMMARY_LABEL_BIN);
            final Set<String> supernodeLabels = new HashSet<>(supernodeCountByLabel.keySet());
            for (final String supernodeLabel : supernodeLabels) {
                final long count = supernodeCountByLabel.getOrDefault(supernodeLabel, 0L);
                supernodeMetadata.put(supernodeLabel, new FireflyPropertiesAndCount(Set.of(), count));
            }
        }

        // If bulk loader is running, get staged partition counts and insert.
        Optional<Map<String, FireflyPropertiesAndCount>> mergeVertexMetadata = Optional.empty();
        if (isBulkLoaderRunning) {
            final Queue<KeyRecord> partitionRecords = getPartitionRecords(db.getConfig().summarySet);
            final Map<String, Long> mergeVertexPartitionCounts = getMergeVertexPartitionCounts(partitionRecords);
            if (!mergeVertexPartitionCounts.isEmpty()) {
                mergeVertexMetadata = Optional.of(new HashMap<>());
                for (final String label : mergeVertexPartitionCounts.keySet()) {
                    mergeVertexMetadata.get().computeIfAbsent(label, k -> new FireflyPropertiesAndCount(Set.of(), 0));
                    mergeVertexMetadata.get().get(label).increment(mergeVertexPartitionCounts.get(label));
                }
            }
            final Map<String, Long> vertexPartitionCounts = getVertexPartitionCounts(partitionRecords);
            final Map<String, Long> edgePartitionCounts = getEdgePartitionCounts(partitionRecords);
            final Map<String, Long> supernodePartitionCounts = getSupernodePartitionCounts(partitionRecords);
            for (final String label : vertexPartitionCounts.keySet()) {
                vertexMetadata.computeIfAbsent(label, k -> new FireflyPropertiesAndCount(Set.of(), 0));
                vertexMetadata.get(label).increment(vertexPartitionCounts.get(label));
            }
            for (final String label : edgePartitionCounts.keySet()) {
                edgeMetadata.computeIfAbsent(label, k -> new FireflyPropertiesAndCount(Set.of(), 0));
                edgeMetadata.get(label).increment(edgePartitionCounts.get(label));
            }
            for (final String label : supernodePartitionCounts.keySet()) {
                supernodeMetadata.computeIfAbsent(label, k -> new FireflyPropertiesAndCount(Set.of(), 0));
                supernodeMetadata.get(label).increment(supernodePartitionCounts.get(label));
            }
        }

        return new FireflyElementMetadata(vertexMetadata, edgeMetadata, supernodeMetadata, mergeVertexMetadata);
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
        if (!db.getConfig().summaryTickerEnabledFlag) {
            return;
        }

        if (lastTickerOutputTime.get() + db.getConfig().summaryTickerIntervalMs > System.currentTimeMillis()) {
            return;
        }

        final FireflyElementMetadata fireflyElementMetadata = getFireflyStatistics();
        LOG.info("Graph summary ticker for " + db.getConfig().graphId + ":\n" + PRETTY_PRINT_FORMAT_LOG,
                fireflyElementMetadata.totalVertexCount(),
                fireflyElementMetadata.vertexCountByLabel(),
                fireflyElementMetadata.vertexPropertiesByLabel(),
                fireflyElementMetadata.totalEdgeCount(),
                fireflyElementMetadata.edgeCountByLabel(),
                fireflyElementMetadata.edgePropertiesByLabel(),
                fireflyElementMetadata.totalSupernodeCount(),
                fireflyElementMetadata.supernodesCountByLabel());
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
                            final AerospikeGraphException lastException = LAST_SUMMARY_TICKER_EXCEPTION.get(this.db.getConfig().graphId);
                            if (lastException == null || lastException.errorCode != e.errorCode) {
                                LAST_SUMMARY_TICKER_EXCEPTION.put(this.db.getConfig().graphId, e);
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
                    // If shutdown is initiated, go through the map and then exit (i.e. skip CountDownLatch).
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
            // Prevent edge case of negative summary counts due to an Aerospike truncate
            final Expression preventNegativeExp = Exp.build(
                    Exp.cond(
                            Exp.lt(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(countInfo.label), Exp.mapBin(SUMMARY_LABEL_BIN)), Exp.val(0)),
                            MapExp.put(updateOnlyPolicy, Exp.val(countInfo.label), Exp.val(0), Exp.mapBin(SUMMARY_LABEL_BIN)),
                            Exp.unknown()
                    )
            );
            final Operation preventNegativeOp = ExpOperation.write(SUMMARY_LABEL_BIN, preventNegativeExp, ExpWriteFlags.EVAL_NO_FAIL);

            db.writeOperate(writePolicy, key, createOp, updateOp, preventNegativeOp);

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
        final Queue<KeyRecord> partitionRecords = getPartitionRecords(db.getConfig().summarySet);
        for (final KeyRecord keyRecord : partitionRecords) {
            if (keyRecord.key.userKey.toString().startsWith(VP_PROPERTY_PREFIX + "PART_"))
                db.delete(keyRecord.key, null);
        }
    }

    public void clearMergeVertexPartitionData() {
        final Queue<KeyRecord> partitionRecords = getPartitionRecords(db.getConfig().summarySet);
        for (final KeyRecord keyRecord : partitionRecords) {
            if (keyRecord.key.userKey.toString().startsWith(VP_PROPERTY_PREFIX + "MERGEV_"))
                db.delete(keyRecord.key, null);
        }
    }

    public void clearEdgePartitionData() {
        final Queue<KeyRecord> partitionRecords = getPartitionRecords(db.getConfig().summarySet);
        for (final KeyRecord keyRecord : partitionRecords) {
            if (keyRecord.key.userKey.toString().startsWith(EP_PROPERTY_PREFIX + "PART_"))
                db.delete(keyRecord.key, null);
        }
    }

    public void clearSupernodePartitionData() {
        final Queue<KeyRecord> partitionRecords = getPartitionRecords(db.getConfig().summarySet);
        for (final KeyRecord keyRecord : partitionRecords) {
            if (keyRecord.key.userKey.toString().startsWith(SP_PROPERTY_PREFIX + "PART_"))
                db.delete(keyRecord.key, null);
        }
    }

    public void forceWrite() {
        countDownLatchBy(Long.MAX_VALUE);
    }

    private void countDownLatchBy(final long count) {
        long iterationsRemaining = count;
        while (COUNTDOWN_LATCH.getCount() > 0 && iterationsRemaining > 0) {
            COUNTDOWN_LATCH.countDown();
            iterationsRemaining--;
        }
    }
}
