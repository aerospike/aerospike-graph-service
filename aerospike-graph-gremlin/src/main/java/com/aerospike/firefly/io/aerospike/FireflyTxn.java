package com.aerospike.firefly.io.aerospike;

import com.aerospike.client.Txn;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;

import java.util.HashSet;
import java.util.Set;

/**
 * Wrapper class for Aerospike MRT Txn in order to process relevant Firefly during TXN abort/commit.
 */
public class FireflyTxn {
    private final FireflyGraph graph;
    public final Txn aerospikeTxn;
    private final Set<FireflyId> recycledEdgeIds;

    public FireflyTxn(final FireflyGraph graph, final Txn txn) {
        this.graph = graph;
        this.aerospikeTxn = txn;
        this.recycledEdgeIds = new HashSet<>();
    }

    public void addIdToRecycle(final FireflyId id) {
        this.recycledEdgeIds.add(id);
    }

    private void commit() {
        this.graph.getBaseGraph().commit(this.graph, this.aerospikeTxn);
        processPostCommit();
    }

    private void rollback() {
        this.graph.getBaseGraph().rollback(this.graph, this.aerospikeTxn);
    }

    private void processPostCommit() {
        for (final FireflyId id : recycledEdgeIds) {
            this.graph.getIdFactory().recycleEdgeId(id);
        }
    }

    static public void commit(final FireflyTxn txn) {
        if (txn != null) {
            txn.commit();
        }
    }

    static public void rollback(final FireflyTxn txn) {
        if (txn != null) {
            txn.rollback();
        }
    }
}
