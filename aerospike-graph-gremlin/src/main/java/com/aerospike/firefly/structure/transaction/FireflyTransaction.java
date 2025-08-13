package com.aerospike.firefly.structure.transaction;

import com.aerospike.client.Txn;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.function.Consumer;

public class FireflyTransaction extends AbstractThreadLocalTransaction {
    static private final Logger LOG = LoggerFactory.getLogger(FireflyTransaction.class);

    private final FireflyGraph graph;
    private final boolean isTxnEnabled;
    private final int timeout;
    private final ThreadLocal<Txn> dbTxn = ThreadLocal.withInitial(() -> null);
    private final ThreadLocal<Boolean> inTxnState = ThreadLocal.withInitial(() -> false);

    public FireflyTransaction(final FireflyGraph g) {
        super(g);
        this.graph = g;
        this.isTxnEnabled = g.getBaseGraph().TRANSACTION_ENABLED;
        this.timeout = g.getBaseGraph().TRANSACTION_TIMEOUT;
    }

    @Override
    protected void doOpen() {
        if (isInTxnState()) {
            LOG.debug("doOpen invoked on Thread: {}", Thread.currentThread().getId());
            final Txn txn = new Txn();
            txn.setTimeout(timeout);
            this.dbTxn.set(txn);
        }
    }

    @Override
    protected void doCommit() throws TransactionException {
        if (isInTxnState()) {
            try {
                LOG.debug("doCommit invoked on Thread: {}", Thread.currentThread().getId());
                this.graph.getBaseGraph().commit(this.dbTxn.get());
            } catch (final Exception e) {
                throw new TransactionException("Exception occurred when commiting transaction.", e);
            }
        }
    }

    @Override
    protected void doRollback() throws TransactionException {
        if (isInTxnState()) {
            try {
                LOG.debug("doRollback invoked on Thread: {}", Thread.currentThread().getId());
                this.graph.getBaseGraph().rollback(this.dbTxn.get());
            } catch (final Exception e) {
                // Reset the txn since a failed rollback should still reset the state to allow new txns.
                this.dbTxn.remove();
                throw new TransactionException("Exception occurred during transaction rollback.", e);
            }
        }
    }

    @Override
    public boolean isOpen() {
        if (isInTxnState()) {
            final Txn currentTxn = this.dbTxn.get();
            LOG.debug("isOpen invoked on Thread: {}", Thread.currentThread().getId());
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

    public void enterTransactionState() {
        this.inTxnState.set(true);
    }

    public void exitTransactionState() {
        this.inTxnState.set(false);
    }

    private boolean isInTxnState() {
        return this.isTxnEnabled && this.inTxnState.get();
    }
}
