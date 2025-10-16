package com.aerospike.firefly.structure.transaction;

import com.aerospike.client.Txn;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;
import java.util.function.Consumer;

public class FireflyTransaction extends AbstractThreadLocalTransaction {
    static private final Logger LOG = LoggerFactory.getLogger(FireflyTransaction.class);

    private final FireflyGraph graph;
    private final boolean isTxnEnabled;
    private final int defaultTimeout;
    private final ThreadLocal<Txn> dbTxn = ThreadLocal.withInitial(() -> null);
    private final ThreadLocal<Boolean> inTxnState = ThreadLocal.withInitial(() -> false);
    private final ThreadLocal<Long> txnTimeout = ThreadLocal.withInitial(() -> -1L);
    private final ThreadLocal<Queue<FireflyId>> committedEdgeIdsToRecycle = ThreadLocal.withInitial(ArrayDeque::new);
    private final ThreadLocal<Queue<FireflyId>> uncommittedEdgeIdsToRecycle = ThreadLocal.withInitial(ArrayDeque::new);

    public FireflyTransaction(final FireflyGraph g) {
        super(g);
        this.graph = g;
        this.isTxnEnabled = g.getBaseGraph().TRANSACTION_ENABLED;
        this.defaultTimeout = g.getBaseGraph().TRANSACTION_TIMEOUT;
    }

    @Override
    protected void doOpen() {
        if (isInTxnState()) {
            LOG.atDebug().addArgument(() -> Thread.currentThread().getId()).log("doOpen invoked on Thread: {}");
            final Txn oldTxn = this.dbTxn.get();
            if (oldTxn != null) {
                this.graph.fireflySummaryUpdater.abortSummaryForTxn(oldTxn);
            }
            final Txn txn = new Txn();
            if (this.txnTimeout.get() != -1L) {
                txn.setTimeout(this.txnTimeout.get().intValue());
            } else {
                txn.setTimeout(this.defaultTimeout);
            }
            this.dbTxn.set(txn);
            this.committedEdgeIdsToRecycle.get().clear();
            this.uncommittedEdgeIdsToRecycle.get().clear();
        }
    }

    @Override
    protected void doCommit() throws TransactionException {
        if (isInTxnState()) {
            try {
                LOG.atDebug().addArgument(() -> Thread.currentThread().getId()).log("doCommit invoked on Thread: {}");
                this.graph.getBaseGraph().commit(this.graph, this.dbTxn.get());
                final Queue<FireflyId> idsToRecycle = this.committedEdgeIdsToRecycle.get();
                while (!idsToRecycle.isEmpty()) {
                    this.graph.getIdFactory().recycleEdgeId(idsToRecycle.poll(), graph, true);
                }
            } catch (final Exception e) {
                throw new TransactionException("Exception occurred when commiting transaction.", e);
            } finally {
                afterTxn();
            }
        }
    }

    @Override
    protected void doRollback() throws TransactionException {
        if (isInTxnState()) {
            try {
                LOG.atDebug().addArgument(() -> Thread.currentThread().getId()).log("doRollback invoked on Thread: {}");
                this.graph.getBaseGraph().rollback(this.graph, this.dbTxn.get());
                final Queue<FireflyId> idsToRecycle = this.uncommittedEdgeIdsToRecycle.get();
                while (!idsToRecycle.isEmpty()) {
                    this.graph.getIdFactory().recycleEdgeId(idsToRecycle.poll(), graph, false);
                }
            } catch (final Exception e) {
                // Reset the txn since a failed rollback should still reset the state to allow new txns.
                this.dbTxn.remove();
                throw new TransactionException("Exception occurred during transaction rollback.", e);
            } finally {
                afterTxn();
            }
        }
    }

    private void afterTxn() {
        this.graph.getIdFactory().recycleCurrentPack();
        this.uncommittedEdgeIdsToRecycle.get().clear();
        this.committedEdgeIdsToRecycle.get().clear();
        this.txnTimeout.remove();
    }

    @Override
    public boolean isOpen() {
        if (isInTxnState()) {
            final Txn currentTxn = this.dbTxn.get();
            LOG.atDebug().addArgument(() -> Thread.currentThread().getId()).log("isOpen invoked on Thread: {}");
            return currentTxn != null && currentTxn.getState() == Txn.State.OPEN;
        }
        return false;
    }

    @Override
    public Transaction onClose(final Consumer<Transaction> consumer) {
        // Fixes wrong Exception in AbstractThreadLocalTransaction
        closeConsumerInternal.set(Optional.ofNullable(consumer).orElseThrow(Transaction.Exceptions::onCloseBehaviorCannotBeNull));
        return this;
    }

    public Txn getCurrentTxn() {
        if (isInTxnState()) {
            return this.dbTxn.get();
        } else {
            return null;
        }
    }

    /**
     * Indicate that the current thread is executing in a transaction context.
     * @param timeout transaction timeout in seconds. -1 to use FireflyGraph's configured default timeout.
     */
    public void enterTransactionState(final long timeout) {
        this.txnTimeout.set(timeout);
        this.inTxnState.set(true);
    }

    public void exitTransactionState() {
        this.inTxnState.set(false);
    }

    public void stageCommittedIdForRecycling(final FireflyId edgeId) {
        this.committedEdgeIdsToRecycle.get().add(edgeId);
    }

    public void stageUncommittedIdForRecycling(final FireflyId edgeId) {
        this.uncommittedEdgeIdsToRecycle.get().add(edgeId);
    }

    private boolean isInTxnState() {
        return this.isTxnEnabled && this.inTxnState.get();
    }
}
