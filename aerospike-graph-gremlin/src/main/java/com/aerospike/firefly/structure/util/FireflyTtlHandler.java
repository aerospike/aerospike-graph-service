package com.aerospike.firefly.structure.util;

import com.aerospike.client.Record;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.FireflyEdgeRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyEdgeFactory;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyEdgeId;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class FireflyTtlHandler implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyTtlHandler.class);
    private static final QueryPolicy INDEX_POLICY = new QueryPolicy();
    static {
        INDEX_POLICY.includeBinData = true;
    }
    public static final String TTL_TIME_KEY = "TTL_TIME_KEY";
    private final FireflyGraph graph;
    private final boolean isTtlEnabled;
    private final int ttlPurgeIntervalMillis;
    private final AtomicLong thisRunTime;
    private final AtomicBoolean timerGetFailed = new AtomicBoolean(false);
    private ScheduledExecutorService scheduler;

    public FireflyTtlHandler(final FireflyGraph graph) {
        this.graph = graph;
        this.isTtlEnabled = graph.getBaseGraph().TTL_ENABLED_FLAG && !graph.bulkLoaderFlag && !graph.getBaseGraph().WARMUP_MODE;
        this.ttlPurgeIntervalMillis = graph.getBaseGraph().TTL_PURGE_INTERVAL_SECONDS * 1000;
        this.thisRunTime = new AtomicLong();
        if (this.isTtlEnabled) {
            this.scheduler = Executors.newSingleThreadScheduledExecutor();
            this.scheduler.schedule(this::startPurge, 0, TimeUnit.MILLISECONDS);
        }
    }

    @Override
    public void close() {
        if (this.isTtlEnabled) {
            this.scheduler.shutdownNow();
        }
    }

    private long getMillisToNextRun() {
        long timeUntilNextInterval = this.thisRunTime.get() + this.ttlPurgeIntervalMillis - System.currentTimeMillis();
        if (timeUntilNextInterval < 0) {
            timeUntilNextInterval = 0;
        }
        return timeUntilNextInterval;
    }

    private void startPurge() {
        this.thisRunTime.set(System.currentTimeMillis());
        long lastRunTime;
        try {
            lastRunTime = this.graph.getBaseGraph().getAndSetTtlTime(thisRunTime.get());
        } catch (final Exception e) {
            if (!timerGetFailed.getAndSet(true)) {
                LOG.warn("Getting last TTL purge runtime failed. Automatically retrying after TTL interval.", e);
            }
            this.scheduler.schedule(this::startPurge, getMillisToNextRun(), TimeUnit.MILLISECONDS);
            return;
        }
        if (timerGetFailed.getAndSet(false)) {
            LOG.warn("Getting last TTL purge runtime succeeded. TTL recovering to normal operation.");
        }
        // Purge TTL elements between the last known purge cutoff and now.
        final long[] normalPurgeCount = purgeElements(lastRunTime, this.thisRunTime.get());
        if (getMillisToNextRun() > 0) {
            // If we have extra time before the next purge, clean up any potential elements before the last known purge cutoff.
            LOG.debug("Extra time available until next TTL purge cycle. Running cleanup cycle.");
            final long[] cleanUpPurgeCount = purgeElements(0, lastRunTime);
            LOG.debug("Cleanup TTL cycle removed " + cleanUpPurgeCount[0] + " expired Vertices and " +
                    cleanUpPurgeCount[1] + " expired Edges.");
        } else {
            LOG.warn("The latest TTL purge had " + normalPurgeCount[0] + " expired Vertices and " + normalPurgeCount[1] +
                    " expired Edges which took longer than the configured time of " + graph.getBaseGraph().TTL_PURGE_INTERVAL_SECONDS +
                    " seconds to remove. Upcoming expiring elements' removal may be delayed.");
        }
        this.scheduler.schedule(this::startPurge, getMillisToNextRun(), TimeUnit.MILLISECONDS);
    }

    private long[] purgeElements(final long startTime, final long endTime) {
        final long[] purgeCount = new long[2];
        purgeCount[0] = purgeVertices(startTime, endTime);
        purgeCount[1] = purgeEdges(startTime, endTime);
        return purgeCount;
    }

    private long purgeVertices(final long startTime, final long endTime) {
        final AerospikeConnection db = this.graph.getBaseGraph();
        long removalCount = 0;
        try {
            final Iterator<KeyRecord> vertexRecordsToDelete = graph.graphQuery.querySIndex(db.VERTEX_AERO_SET,
                    db.TTL_VERTEX_INDEX_NAME, Filter.range(db.TTL_BIN, startTime, endTime), INDEX_POLICY);
            while (vertexRecordsToDelete.hasNext()) {
                final KeyRecord vertexRecord = vertexRecordsToDelete.next();
                final FireflyVertex vertex = this.graph.vertexFromRecord(vertexRecord);
                if (vertex != null) {
                    try {
                        vertex.remove();
                        removalCount++;
                    } catch (final AerospikeGraphException e) {
                        LOG.error("Unexpected error occurred when removing TTL Vertex ID {}: {}", vertex.id(), e.getMessage());
                    }
                }
            }
        } catch (final AerospikeGraphException e) {
            LOG.error("Unexpected error occurred when running index to grab TTL expired Vertices: {}", e.getMessage());
        } catch (final Exception e) {
            LOG.error("Unexpected exception when TTL purging Vertices.", e);
        }
        LOG.debug("TTL purge removed {}} expired Vertices.", removalCount);
        return removalCount;
    }

    private long purgeEdges(final long startTime, final long endTime) {
        final AerospikeConnection db = this.graph.getBaseGraph();
        long removalCount = 0;
        try {
            final Iterator<KeyRecord> edgesToDelete = graph.graphQuery.querySIndex(db.EDGE_AERO_SET,
                    db.TTL_EDGE_INDEX_NAME, Filter.range(db.TTL_BIN, IndexCollectionType.MAPVALUES, startTime, endTime),
                    INDEX_POLICY);
            long currentEdgeDeleteTime = System.currentTimeMillis();
            while (edgesToDelete.hasNext()) {
                final Record record = edgesToDelete.next().record;
                final FireflyEdgeRecord edgeRecord = new FireflyEdgeRecord(record, graph.getBaseGraph());
                final Map<?, Long> edgeTtls = (Map<?, Long>) record.getMap(db.TTL_BIN);
                for (final Map.Entry<?, Long> edgeTtl : edgeTtls.entrySet()) {
                    final long expiryTime = edgeTtl.getValue();
                    // Need this check since phat edge TTL bin map can contain entries outside of index range
                    if (expiryTime <= currentEdgeDeleteTime) {
                        final FireflyEdgeId edgeId = db.getIdFactory().createEdgeId(edgeTtl.getKey());
                        final FireflyEdge edge = FireflyEdgeFactory.create(edgeId, edgeRecord, graph);
                        if (edge != null) {
                            try {
                                edge.remove();
                                currentEdgeDeleteTime = System.currentTimeMillis();
                                removalCount++;
                            } catch (final AerospikeGraphException e) {
                                LOG.error("Unexpected error occurred when removing TTL Edge ID {}: {}", edge.id(), e.getMessage());
                            }
                        }
                    }
                }
            }
        } catch (final AerospikeGraphException e) {
            LOG.error("Unexpected error occurred when running index to grab TTL expired Edges: {}", e.getMessage());
        } catch (final Exception e) {
            LOG.error("Unexpected exception when TTL purging Edges.", e);
        }
        LOG.debug("TTL purge removed {} expired Edges.", removalCount);
        return removalCount;
    }
}
