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
    private final Set<FireflyId> committedEdgeIdsToRecycle;
    private final Set<FireflyId> uncommittedEdgeIdsToRecycle;

    public FireflyTxn(final FireflyGraph graph, final Txn txn) {
        this.graph = graph;
        this.aerospikeTxn = txn;
        this.committedEdgeIdsToRecycle = new HashSet<>();
        this.uncommittedEdgeIdsToRecycle = new HashSet<>();
    }

    public void stageCommittedIdForRecycling(final FireflyId id) {
        this.committedEdgeIdsToRecycle.add(id);
    }

    public void stageUncommittedIdForRecycling(final FireflyId id) {
        this.uncommittedEdgeIdsToRecycle.add(id);
    }

    private void commit() {
        this.graph.getBaseGraph().commit(this.graph, this.aerospikeTxn);
        processPostCommit();
    }

    private void processPostCommit() {
        for (final FireflyId id : committedEdgeIdsToRecycle) {
            this.graph.getIdFactory().recycleEdgeId(id, this.graph, true);
        }
    }

    private void rollback() {
        this.graph.getBaseGraph().rollback(this.graph, this.aerospikeTxn);
        processPostRollback();
    }

    private void processPostRollback() {
        for (final FireflyId id : uncommittedEdgeIdsToRecycle) {
            this.graph.getIdFactory().recycleEdgeId(id, this.graph, false);
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
