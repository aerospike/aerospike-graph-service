package com.aerospike.firefly.bulkloader.util;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.cdt.ListOperation;
import com.aerospike.client.cdt.ListOrder;
import com.aerospike.client.cdt.ListPolicy;
import com.aerospike.client.cdt.ListWriteFlags;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateReadVertices;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.process.call.bulkload.utils.exception.FireflyLoadingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

public class RecoveryUtil {
    private static final Logger LOGGER = LoggerFactory.getLogger(RecoveryUtil.class);

    public enum RecoveryState {
        DETECT_SUPERNODES,
        VERTEX_WRITE,
        VERTEX_VERIFY,
        EDGE_WRITE,
        EDGE_VERIFY
    }

    public static void truncate(final AerospikeConnection db) {
        try {
            db.getClient().truncate(null, db.getNamespace(), db.BULK_LOAD_RECOVERY_VERTEX_SET, null);
            db.getClient().truncate(null, db.getNamespace(), db.BULK_LOAD_RECOVERY_EDGE_SET, null);
            db.getClient().truncate(null, db.getNamespace(), db.BULK_LOAD_RECOVERY_SUPERNODE_SET, null);
            db.getClient().truncate(null, db.getNamespace(), db.BULK_LOAD_RECOVERY_STATE_SET, null);
            Thread.sleep(1);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void writePartitionComplete(final AerospikeConnection db, final int partitionId, final String set) {
        final String keyId = String.valueOf(partitionId / 100);
        final Key key = new Key(db.namespace, set, Value.get(keyId));

        final ListPolicy createListOnlyPolicy = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL);
        final Operation createOp = ListOperation.create(db.BULK_LOAD_RECOVERY_BIN, ListOrder.UNORDERED, false);
        final Operation updateOp = ListOperation.append(createListOnlyPolicy, db.BULK_LOAD_RECOVERY_BIN, Value.get(partitionId));
        final WritePolicy writePolicy = new WritePolicy();
        db.configureWritePolicy(writePolicy);

        // TODO: Retry logic.
        try {
            db.writeOperate(writePolicy, key, createOp, updateOp);
        } catch (AerospikeException e) {
            throw new FireflyLoadingException(e);
        }
    }

    public static void writeVertexPartitionComplete(final AerospikeConnection db, final int partitionId) {
        // write complete vertex partition id to aerospike
        writePartitionComplete(db, partitionId, db.BULK_LOAD_RECOVERY_VERTEX_SET);
    }

    public static void writeEdgePartitionComplete(final AerospikeConnection db, final int partitionId) {
        // write complete edge partition id to aerospike
        writePartitionComplete(db, partitionId, db.BULK_LOAD_RECOVERY_EDGE_SET);
    }

    public static void writeSupernodeList(final AerospikeConnection db, final Set<Object> supernodes) {
        // Create policy and configure.
        final ListPolicy createListOnlyPolicy = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL);
        final Operation createOp = ListOperation.create(db.BULK_LOAD_RECOVERY_BIN, ListOrder.UNORDERED, false);
        final WritePolicy writePolicy = new WritePolicy();
        db.configureWritePolicy(writePolicy);

        // Break set of supernodes into groups of 1k and write to aerospike.
        final Object[] supernodeArray = supernodes.toArray();
        for (int i = 0; i < supernodeArray.length; i += 1000) {
            final Object[] supernodeBatch = new Object[Math.min(1000, supernodeArray.length - i)];
            System.arraycopy(supernodeArray, i, supernodeBatch, 0, supernodeBatch.length);
            final Key key = new Key(db.namespace, db.BULK_LOAD_RECOVERY_SUPERNODE_SET, Value.get(i / 1000));
            List<Value> values = new ArrayList<>();
            for (Object supernode : supernodeBatch) {
                values.add(Value.get(supernode));
            }
            final Operation updateOp = ListOperation.appendItems(createListOnlyPolicy, db.BULK_LOAD_RECOVERY_BIN, values);

            // TODO: Retry logic.
            try {
                db.writeOperate(writePolicy, key, createOp, updateOp);
            } catch (AerospikeException e) {
                throw new FireflyLoadingException(e);
            }
        }
    }

    public static void updateState(final AerospikeConnection db, final RecoveryState state) {
        // Create policy and configure.
        final Key key = new Key(db.namespace, db.BULK_LOAD_RECOVERY_STATE_SET, "state");
        final Bin bin = new Bin(db.BULK_LOAD_RECOVERY_BIN, state.name());
        final Operation operation = Operation.put(bin);
        final WritePolicy writePolicy = new WritePolicy();
        db.configureWritePolicy(writePolicy);

        // TODO: Retry logic.
        try {
            db.writeOperate(writePolicy, key, operation);
        } catch (AerospikeException e) {
            throw new FireflyLoadingException(e);
        }
    }

    public static void updateVertexRecovery(final AerospikeConnection db, final int partitionCount) {
        // Create policy and configure.
        final Key key = new Key(db.namespace, db.BULK_LOAD_RECOVERY_STATE_SET, "vertex_partition_count");
        final Bin bin = new Bin(db.BULK_LOAD_RECOVERY_BIN, partitionCount);
        final Operation operation = Operation.put(bin);
        final WritePolicy writePolicy = new WritePolicy();
        db.configureWritePolicy(writePolicy);

        // TODO: Retry logic.
        try {
            db.writeOperate(writePolicy, key, operation);
        } catch (AerospikeException e) {
            throw new FireflyLoadingException(e);
        }
    }

    public static void updateEdgeRecovery(final AerospikeConnection db, final int partition_count) {
        // Create policy and configure.
        final Key key = new Key(db.namespace, db.BULK_LOAD_RECOVERY_STATE_SET, "edge_partition_count");
        final Bin bin = new Bin(db.BULK_LOAD_RECOVERY_BIN, partition_count);
        final Operation operation = Operation.put(bin);
        final WritePolicy writePolicy = new WritePolicy();
        db.configureWritePolicy(writePolicy);

        // TODO: Retry logic.
        try {
            db.writeOperate(writePolicy, key, operation);
        } catch (AerospikeException e) {
            throw new FireflyLoadingException(e);
        }
    }

    private static void recoverPartitions(final RecoveryRecordSequenceListener listener,
                                          final AerospikeConnection db,
                                          final ScanPolicy scanPolicy,
                                          final String set,
                                          final RecoveryRecordSequenceListener.RecoveryMode mode) {
        listener.mode = mode;
        listener.latch = new CountDownLatch(1);
        listener.reset();
        db.getClient().scanAll(db.getEventLoops().next(), listener, scanPolicy, db.getNamespace(), set);
        try {
            // Wait up to 5 minutes for the scan to complete.
            listener.latch.await(5 * 60 * 1000, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (final InterruptedException ignored) {
        }
        if (listener.getFailed() || !listener.isDone()) {
            throw new RuntimeException("Failed to recover from checkpoint");
        }
    }

    private static String recoverState(final AerospikeConnection db) {
        final Key key = new Key(db.namespace, db.BULK_LOAD_RECOVERY_STATE_SET, "state");
        final Policy readPolicy = new Policy();
        db.configureReadPolicy(readPolicy);

        // TODO: Retry logic.
        try {
            final Record record = db.client.get(readPolicy, key);
            if (record != null) {
                return record.getString(db.BULK_LOAD_RECOVERY_BIN);
            }
            return null;
        } catch (AerospikeException e) {
            throw new FireflyLoadingException(e);
        }
    }

    public static int recoverVertexPartitionCount(final AerospikeConnection db) {
        final Key key = new Key(db.namespace, db.BULK_LOAD_RECOVERY_STATE_SET, "vertex_partition_count");
        final Policy readPolicy = new Policy();
        db.configureReadPolicy(readPolicy);

        // TODO: Retry logic.
        try {
            final Record record = db.client.get(readPolicy, key);
            if (record != null) {
                return record.getInt(db.BULK_LOAD_RECOVERY_BIN);
            }
            return -1;
        } catch (AerospikeException e) {
            throw new FireflyLoadingException(e);
        }
    }

    public static String getEdgeRecoveryDirectory(final String tempDirectory, final String separator) {
        if (tempDirectory.endsWith(separator) || tempDirectory.endsWith("/")) {
            return tempDirectory + "recovery" + separator + "edge";
        } else {
            return tempDirectory + separator + "recovery" + separator + "edge";
        }
    }

    public static int recoverEdgePartitionCount(final AerospikeConnection db) {
        final Key key = new Key(db.namespace, db.BULK_LOAD_RECOVERY_STATE_SET, "edge_partition_count");
        final Policy readPolicy = new Policy();
        db.configureReadPolicy(readPolicy);

        // TODO: Retry logic.
        try {
            final Record record = db.client.get(readPolicy, key);
            if (record != null) {
                return record.getInt(db.BULK_LOAD_RECOVERY_BIN);
            }
            return -1;
        } catch (AerospikeException e) {
            throw new FireflyLoadingException(e);
        }
    }

    private static String recoverVertexDirectory(final AerospikeConnection db) {
        final Key key = new Key(db.namespace, db.BULK_LOAD_RECOVERY_STATE_SET, "vertex");
        final Policy readPolicy = new Policy();
        db.configureReadPolicy(readPolicy);

        // TODO: Retry logic.
        try {
            final Record record = db.client.get(readPolicy, key);
            if (record != null) {
                return record.getString(db.BULK_LOAD_RECOVERY_BIN);
            }
            return null;
        } catch (AerospikeException e) {
            throw new FireflyLoadingException(e);
        }
    }

    public static RecoveryInfo recover(final AerospikeConnection db) {
        final ScanPolicy scanPolicy = new ScanPolicy();
        db.configureScanPolicy(scanPolicy);
        scanPolicy.sendKey = true;
        final RecoveryRecordSequenceListener listener = new RecoveryRecordSequenceListener(db);
        recoverPartitions(listener, db, scanPolicy, db.BULK_LOAD_RECOVERY_VERTEX_SET, RecoveryRecordSequenceListener.RecoveryMode.VERTEX);
        recoverPartitions(listener, db, scanPolicy, db.BULK_LOAD_RECOVERY_EDGE_SET, RecoveryRecordSequenceListener.RecoveryMode.EDGE);
        recoverPartitions(listener, db, scanPolicy, db.BULK_LOAD_RECOVERY_SUPERNODE_SET, RecoveryRecordSequenceListener.RecoveryMode.SUPERNODE);
        return new RecoveryInfo(
                listener.getVertexPartitions(),
                listener.getEdgePartitions(),
                listener.getSupernodes(),
                recoverState(db),
                recoverVertexPartitionCount(db),
                recoverEdgePartitionCount(db));
    }

    static class RecoveryRecordSequenceListener implements RecordSequenceListener {
        final Set<Long> vertexPartitions = new HashSet<>();
        final Set<Long> edgePartitions = new HashSet<>();
        final Set<Object> supernodes = new HashSet<>();
        final AerospikeConnection db;
        public CountDownLatch latch;
        boolean failed = false;
        boolean done = false;
        public RecoveryMode mode;

        public enum RecoveryMode {
            VERTEX,
            EDGE,
            SUPERNODE
        }

        RecoveryRecordSequenceListener(final AerospikeConnection db) {
            this.db = db;
            mode = RecoveryMode.VERTEX;
        }

        @Override
        public void onRecord(final Key key, final Record record) throws AerospikeException {
            if (RecoveryMode.VERTEX.equals(mode)) {
                final Set<Long> partitionIds = (Set) ((List) record.bins.get(db.BULK_LOAD_RECOVERY_BIN)).stream().collect(Collectors.toSet());
                vertexPartitions.addAll(partitionIds);
            } else if (RecoveryMode.EDGE.equals(mode)) {
                final Set<Long> partitionIds = (Set) ((List) record.bins.get(db.BULK_LOAD_RECOVERY_BIN)).stream().collect(Collectors.toSet());
                edgePartitions.addAll(partitionIds);
            } else if (RecoveryMode.SUPERNODE.equals(mode)) {
                final Set<Object> partitionIds = (Set) ((List) record.bins.get(db.BULK_LOAD_RECOVERY_BIN)).stream().collect(Collectors.toSet());
                supernodes.addAll(partitionIds);
            } else {
                failed = true;
                latch.countDown();
                throw new RuntimeException("Invalid recovery mode.");
            }
        }

        @Override
        public void onSuccess() {
            done = true;
            latch.countDown();
        }

        @Override
        public void onFailure(final AerospikeException ae) {
            failed = true;
            LOGGER.error("Failed to recover from checkpoint", ae);
            latch.countDown();
        }

        public boolean getFailed() {
            return failed;
        }

        public Set<Long> getVertexPartitions() {
            return vertexPartitions;
        }

        public Set<Long> getEdgePartitions() {
            return edgePartitions;
        }

        public Set<Object> getSupernodes() {
            return supernodes;
        }

        public void reset() {
            done = false;
        }

        public boolean isDone() {
            return done;
        }
    }

    public static class RecoveryInfo {
        private Set<Long> vertexPartitions;
        private Set<Long> edgePartitions;
        private Set<Object> supernodes;
        private String state;
        private int vertexPartitionCount;
        private int edgePartitionCount;

        public RecoveryInfo(final Set<Long> vertexPartitions,
                            final Set<Long> edgePartitions,
                            final Set<Object> supernodes,
                            final String state,
                            final int vertexPartitionCount,
                            final int edgePartitionCount) {
            this.vertexPartitions = vertexPartitions;
            this.edgePartitions = edgePartitions;
            this.supernodes = supernodes;
            this.state = state;
            this.vertexPartitionCount = vertexPartitionCount;
            this.edgePartitionCount = edgePartitionCount;
        }

        public Set<Long> getVertexPartitions() {
            return vertexPartitions;
        }

        public Set<Long> getEdgePartitions() {
            return edgePartitions;
        }

        public Set<Object> getSupernodes() {
            return supernodes;
        }

        public String getState() {
            return state;
        }

        public int getVertexPartitionCount() {
            return vertexPartitionCount;
        }

        public int getEdgePartitionCount() {
            return edgePartitionCount;
        }
    }
}
