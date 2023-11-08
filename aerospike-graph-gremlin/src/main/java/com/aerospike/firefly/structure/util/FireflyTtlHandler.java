package com.aerospike.firefly.structure.util;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.RecordExistsAction;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
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
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.aerospike.firefly.runtime.exceptions.EdgeRecordSizeExceededException.getUserIdString;

public class FireflyTtlHandler implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyTtlHandler.class);
    private static final String TIMER_NAME = "TTL_TIMER";
    private final boolean isTtlEnabled;
    private ScheduledExecutorService scheduler;
    private Timer timer;

    public FireflyTtlHandler(final FireflyGraph graph) {
        this.isTtlEnabled = graph.getBaseGraph().TTL_ENABLED_FLAG;
        if (this.isTtlEnabled) {
            this.scheduler = Executors.newSingleThreadScheduledExecutor();
            this.timer = new Timer(TIMER_NAME, true);
            timer.schedule(new TtlTimerTask(graph, this.scheduler), graph.getBaseGraph().TTL_PURGE_INTERVAL,
                    graph.getBaseGraph().TTL_PURGE_INTERVAL);
        }
    }

    @Override
    public void close() {
        if (this.isTtlEnabled) {
            this.timer.cancel();
            this.scheduler.shutdownNow();
        }
    }

    private static class TtlTimerTask extends TimerTask {
        private static final String TTL_LOCK_KEY = "TTL_LOCK_KEY";
        private static final String TTL_LOCK_BIN = "CREATED_ON";
        private static final QueryPolicy INDEX_POLICY = new QueryPolicy();
        static {
            INDEX_POLICY.sendKey = true;
            INDEX_POLICY.includeBinData = true;
        }
        private final FireflyGraph graph;
        private final AerospikeConnection db;
        private final ScheduledExecutorService scheduler;
        private final Key lockKey;
        private final WritePolicy acquireTtlLockPolicy;
        private int lastFailureCode = ResultCode.OK;

        private TtlTimerTask(final FireflyGraph graph, final ScheduledExecutorService scheduler) {
            this.graph = graph;
            this.db = graph.getBaseGraph();
            this.scheduler = scheduler;
            this.lockKey = new Key(db.getNamespace(), db.GRAPH_METADATA_SET, TTL_LOCK_KEY);

            final WritePolicy policy = new WritePolicy();
            policy.recordExistsAction = RecordExistsAction.CREATE_ONLY;
            // TTL on Aerospike is in seconds. Offset by 1 second to prevent jitter issues.
            policy.expiration = (db.TTL_PURGE_INTERVAL / 1000) - 1;
            this.acquireTtlLockPolicy = policy;
        }

        @Override
        public void run() {
            // Purge Vertices first since this may purge some Edges.
            if (tryAcquireLock()) {
                scheduleVertexDeletes();
                scheduleEdgeDeletes();
            }
        }

        private boolean tryAcquireLock() {
            try {
                final Operation createLockRecord = Operation.put(new Bin(TTL_LOCK_BIN, System.currentTimeMillis()));
                this.db.getClient().operate(this.acquireTtlLockPolicy, this.lockKey, createLockRecord);
                LOG.debug("Successfully grabbed TTL element purge lock. Starting TTL element purge.");
                return true;
            } catch (final AerospikeException ae) {
                if (ae.getResultCode() != ResultCode.KEY_EXISTS_ERROR && this.lastFailureCode != ae.getResultCode()) {
                    this.lastFailureCode = ae.getResultCode();
                    LOG.error("Encountered unexpected error when initializing TTL element purge.", ae);
                }
                LOG.debug("Could not grab TTL element purge lock. Sleeping until next interval.");
                return false;
            }
        }

        private void scheduleVertexDeletes() {
            final Iterator<KeyRecord> vertexRecordsToDelete = db.queryIndex(db.VERTEX_AERO_SET,
                    db.TTL_VERTEX_INDEX_NAME, Filter.range(db.TTL_BIN,
                    0, System.currentTimeMillis() + db.TTL_PURGE_INTERVAL), INDEX_POLICY);
            int expiriesScheduled = 0;
            while (vertexRecordsToDelete.hasNext()) {
                final KeyRecord vertexRecord = vertexRecordsToDelete.next();
                final long expiryTime = vertexRecord.record.getLong(db.TTL_BIN);
                final long remainingTime = expiryTime - System.currentTimeMillis();
                expiriesScheduled++;
                scheduler.schedule(() -> {
                    final FireflyVertex vertex = graph.vertexFromRecord(vertexRecord);
                    try {
                        vertex.remove();
                    } catch (final AerospikeException e) {
                        LOG.error("Unexpected error occurred when removing TTL Vertex ID {}: {}", vertex.id, e);
                    }
                }, remainingTime, TimeUnit.MILLISECONDS);
            }
            if (expiriesScheduled < db.TTL_PURGE_INTERVAL / 100) {
                LOG.debug("Scheduled " + expiriesScheduled + " Vertex TTL expiries within the next "
                        + db.TTL_PURGE_INTERVAL + "ms.");
            } else {
                LOG.warn("Scheduled " + expiriesScheduled + " Vertex TTL expiries within the next "
                        + db.TTL_PURGE_INTERVAL + "ms. System may experience stress due to expiry frequency.");
            }
        }

        private void scheduleEdgeDeletes() {
            final long timeRangeMaximum = System.currentTimeMillis() + db.TTL_PURGE_INTERVAL;
            final Iterator<KeyRecord> edgesToDelete = db.queryIndex(db.EDGE_AERO_SET, db.TTL_EDGE_INDEX_NAME,
                    Filter.range(db.TTL_BIN, IndexCollectionType.MAPVALUES, 0, timeRangeMaximum), INDEX_POLICY);
            int expiriesScheduled = 0;
            while (edgesToDelete.hasNext()) {
                final Record edgeRecord = edgesToDelete.next().record;
                final Map<?, Long> edgeTtls = (Map<?, Long>) edgeRecord.getMap(db.TTL_BIN);
                for (final Map.Entry<?, Long> edgeTtl : edgeTtls.entrySet()) {
                    final long expiryTime = edgeTtl.getValue();
                    // Need this check since phat edge TTL bin map can contain entries outside of index range
                    if (expiryTime <= timeRangeMaximum) {
                        final FireflyId edgeId = new FireflyPhatEdgeId((ByteBuffer) edgeTtl.getKey(),
                                db.PHAT_EDGE_SIZE, db.EDGE_AERO_SET);
                        final FireflyEdge edge = FireflyEdge.FireflyEdgeFactory.create(edgeId, edgeRecord, graph);
                        final long remainingTime = expiryTime - System.currentTimeMillis();
                        expiriesScheduled++;
                        scheduler.schedule(() -> {
                            try {
                                edge.remove();
                            } catch (AerospikeException e) {
                                LOG.error("Unexpected error occurred when removing TTL Edge ID {}: {}",
                                        getUserIdString(edge.id.getUserId()), e);
                            }
                        }, remainingTime, TimeUnit.MILLISECONDS);
                    }
                }
            }
            if (expiriesScheduled < db.TTL_PURGE_INTERVAL / 100) {
                LOG.debug("Scheduled " + expiriesScheduled + " Edge TTL expiries within the next "
                        + db.TTL_PURGE_INTERVAL + "ms.");
            } else {
                LOG.warn("Scheduled " + expiriesScheduled + " Edge TTL expiries within the next "
                        + db.TTL_PURGE_INTERVAL + "ms. System may experience stress due to expiry frequency.");
            }
        }
    }
}
