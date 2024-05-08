package com.aerospike.firefly.structure.util;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Record;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.query.GraphQuery;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.nio.ByteBuffer;
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
        INDEX_POLICY.sendKey = true;
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
        this.isTtlEnabled = graph.getBaseGraph().TTL_ENABLED_FLAG;
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
                    " to remove. Upcoming expiring elements' removal may be delayed.");
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
            final Iterator<KeyRecord> vertexRecordsToDelete = GraphQuery.create(graph).querySIndex(db.VERTEX_AERO_SET,
                    db.TTL_VERTEX_INDEX_NAME, Filter.range(db.TTL_BIN, startTime, endTime), INDEX_POLICY);
            while (vertexRecordsToDelete.hasNext()) {
                final KeyRecord vertexRecord = vertexRecordsToDelete.next();
                final FireflyVertex vertex = this.graph.vertexFromRecord(vertexRecord);
                if (vertex != null) {
                    try {
                        vertex.remove();
                        removalCount++;
                    } catch (final AerospikeException e) {
                        LOG.error("Unexpected error occurred when removing TTL Vertex ID {}: {}", vertex.id(), e);
                    }
                }
            }
        } catch (final AerospikeException e) {
            LOG.error("Unexpected error occurred when running SIndex to grab TTL expired Vertices.", e);
        }
        LOG.debug("TTL purge removed " + removalCount + " expired Vertices.");
        return removalCount;
    }

    private long purgeEdges(final long startTime, final long endTime) {
        final AerospikeConnection db = this.graph.getBaseGraph();
        long removalCount = 0;
        try {
            final Iterator<KeyRecord> edgesToDelete = GraphQuery.create(graph).querySIndex(db.EDGE_AERO_SET,
                    db.TTL_EDGE_INDEX_NAME, Filter.range(db.TTL_BIN, IndexCollectionType.MAPVALUES, startTime, endTime),
                    INDEX_POLICY);
            while (edgesToDelete.hasNext()) {
                final Record edgeRecord = edgesToDelete.next().record;
                final Map<?, Long> edgeTtls = (Map<?, Long>) edgeRecord.getMap(db.TTL_BIN);
                for (final Map.Entry<?, Long> edgeTtl : edgeTtls.entrySet()) {
                    final long expiryTime = edgeTtl.getValue();
                    // Need this check since phat edge TTL bin map can contain entries outside of index range
                    if (expiryTime <= this.thisRunTime.get()) {
                        final FireflyId edgeId = new FireflyPhatEdgeId((ByteBuffer) edgeTtl.getKey(),
                                db.PHAT_EDGE_SIZE, db.EDGE_AERO_SET);
                        final FireflyEdge edge = FireflyEdge.FireflyEdgeFactory.create(edgeId, edgeRecord, graph);
                        if (edge != null) {
                            try {
                                edge.remove();
                                removalCount++;
                            } catch (final AerospikeException e) {
                                LOG.error("Unexpected error occurred when removing TTL Edge ID {}: {}", edge.id(), e);
                            }
                        }
                    }
                }
            }
        } catch (final AerospikeException e) {
            LOG.error("Unexpected error occurred when running SIndex to grab TTL expired Edges.", e);
        }
        LOG.debug("TTL purge removed " + removalCount + " expired Edges.");
        return removalCount;
    }
}
