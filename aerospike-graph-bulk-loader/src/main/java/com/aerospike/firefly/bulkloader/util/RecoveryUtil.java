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
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.policy.WritePolicy;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.process.call.bulkload.utils.exception.FireflyLoadingException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static com.aerospike.firefly.io.aerospike.AerospikeConnection.DefaultAerospikeClientProvider.client;

public class RecoveryUtil {

    private static final String VERTEX_PREFIX = "vertex_";
    private static final String EDGE_PREFIX = "edge_";
    private static final String SUPERNODE_PREFIX = "supernode_";

    public static void writePartitionComplete(final AerospikeConnection db, final int partitionId, final String type) {
        final String keyId = type + partitionId / 100;
        final Key key = new Key(db.namespace, db.BULK_LOAD_RECOVERY_SET, Value.get(keyId));


        final ListPolicy createListOnlyPolicy = new ListPolicy(ListOrder.UNORDERED, ListWriteFlags.ADD_UNIQUE | ListWriteFlags.NO_FAIL);
        final Operation createOp = ListOperation.create(db.BULK_LOAD_RECOVERY_BIN, ListOrder.UNORDERED, false);
        final Operation updateOp = ListOperation.append(createListOnlyPolicy, db.BULK_LOAD_RECOVERY_BIN, Value.get(partitionId));
        final Bin bin = new Bin(db.USER_KEY_BIN, Value.get(keyId));
        final Operation addKeyOp = Operation.put(bin);
        final WritePolicy writePolicy = new WritePolicy();
        db.configureWritePolicy(writePolicy);

        // TODO: Retry logic.
        try {
            db.writeOperate(writePolicy, key, createOp, updateOp, addKeyOp);
        } catch (AerospikeException e) {
            throw new FireflyLoadingException(e);
        }
    }

    public static void writeVertexPartitionComplete(final AerospikeConnection db, final int partitionId) {
        // write complete vertex partition id to aerospike
        writePartitionComplete(db, partitionId, VERTEX_PREFIX);
    }

    public static void writeEdgePartitionComplete(final AerospikeConnection db, final int partitionId) {
        // write complete edge partition id to aerospike
        writePartitionComplete(db, partitionId, EDGE_PREFIX);
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
            final Key key = new Key(db.namespace, db.BULK_LOAD_RECOVERY_SET, Value.get(SUPERNODE_PREFIX + i / 1000));
            List<Value> values = new ArrayList<>();
            for (Object supernode : supernodeBatch) {
                values.add(Value.get(supernode));
            }
            final Operation updateOp = ListOperation.appendItems(createListOnlyPolicy, db.BULK_LOAD_RECOVERY_BIN, values);
            final Bin bin = new Bin(db.USER_KEY_BIN, Value.get(SUPERNODE_PREFIX + i / 1000));
            final Operation addKeyOp = Operation.put(bin);

            // TODO: Retry logic.
            try {
                db.writeOperate(writePolicy, key, createOp, updateOp, addKeyOp);
            } catch (AerospikeException e) {
                throw new FireflyLoadingException(e);
            }
        }
    }

    public static RecoveryInfo recover(final AerospikeConnection db) {
        final ScanPolicy scanPolicy = new ScanPolicy();
        db.configureScanPolicy(scanPolicy);
        scanPolicy.sendKey = true;
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final RecoveryRecordSequenceListener listener = new RecoveryRecordSequenceListener(db, countDownLatch);
        client.scanAll(db.getEventLoops().next(), listener, scanPolicy, db.getNamespace(), db.BULK_LOAD_RECOVERY_SET);

        try {
            // Wait up to 5 minutes for the scan to complete.
            countDownLatch.await(5 * 60 * 1000, java.util.concurrent.TimeUnit.MILLISECONDS);
        } catch (final InterruptedException ignored) {
        }

        if (listener.getFailed()) {
            throw new RuntimeException("Failed to recover from checkpoint");
        }

        return new RecoveryInfo(listener.getVertexPartitions(), listener.getEdgePartitions(), listener.getSupernodes());
    }

    static class RecoveryRecordSequenceListener implements RecordSequenceListener {
        final Set<Long> vertexPartitions = new HashSet<>();
        final Set<Long> edgePartitions = new HashSet<>();
        final Set<Object> supernodes = new HashSet<>();
        final AerospikeConnection db;
        final CountDownLatch latch;
        boolean failed = false;

        RecoveryRecordSequenceListener(final AerospikeConnection db, final CountDownLatch latch) {
            this.db = db;
            this.latch = latch;
        }

        @Override
        public void onRecord(final Key key, final Record record) throws AerospikeException {
            final String keyId = record.getString(db.USER_KEY_BIN);
            if (keyId.startsWith(VERTEX_PREFIX)) {
                final Set<Long> partitionIds = (Set) ((List) record.bins.get(db.BULK_LOAD_RECOVERY_BIN)).stream().collect(Collectors.toSet());
                vertexPartitions.addAll(partitionIds);
            } else if (keyId.startsWith(EDGE_PREFIX)) {
                final Set<Long> partitionIds = (Set) ((List) record.bins.get(db.BULK_LOAD_RECOVERY_BIN)).stream().collect(Collectors.toSet());
                edgePartitions.addAll(partitionIds);
            } else if (keyId.startsWith(SUPERNODE_PREFIX)) {
                final Set<Object> partitionIds = (Set) ((List) record.bins.get(db.BULK_LOAD_RECOVERY_BIN)).stream().collect(Collectors.toSet());
                supernodes.addAll(partitionIds);
            }
        }

        @Override
        public void onSuccess() {
            latch.countDown();
        }

        @Override
        public void onFailure(final AerospikeException ae) {
            failed = true;
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
    }

    public static class RecoveryInfo {
        private Set<Long> vertexPartitions;
        private Set<Long> edgePartitions;
        private Set<Object> supernodes;

        public RecoveryInfo(final Set<Long> vertexPartitions, final Set<Long> edgePartitions, final Set<Object> supernodes) {
            this.vertexPartitions = vertexPartitions;
            this.edgePartitions = edgePartitions;
            this.supernodes = supernodes;
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
    }
}
