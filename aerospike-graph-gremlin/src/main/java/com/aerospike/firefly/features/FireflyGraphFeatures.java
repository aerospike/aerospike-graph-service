package com.aerospike.firefly.features;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.io.GraphReader;
import org.apache.tinkerpop.gremlin.structure.io.GraphWriter;

class FireflyGraphFeatures implements Graph.Features.GraphFeatures {
    private final boolean transactionsEnabled;

    FireflyGraphFeatures(final boolean transactionsEnabled) {
        this.transactionsEnabled = transactionsEnabled;
    }

    /**
     * Determines if the {@code Graph} implementation supports {@link GraphComputer} based processing.
     */
    @Override
    public boolean supportsComputer() {
        return true;
    }

    /**
     * Determines if the {@code Graph} implementation supports persisting its contents natively to disk.
     * This feature does not refer to every graph's ability to write to disk via the Gremlin IO packages
     * (.e.g. GraphML), unless the graph natively persists to disk via those options somehow.  For example,
     * TinkerGraph does not support this feature as it is a pure in-sideEffects graph.
     */
    @Override
    public boolean supportsPersistence() {
        return true;
    }

    /**
     * Determines if the {@code Graph} implementation supports more than one connection to the same instance
     * at the same time.  For example, Neo4j embedded does not support this feature because concurrent
     * access to the same database files by multiple instances is not possible.  However, Neo4j HA could
     * support this feature as each new {@code Graph} instance coordinates with the Neo4j cluster allowing
     * multiple instances to operate on the same database.
     */
    @Override
    public boolean supportsConcurrentAccess() {
        return true;
    }

    /**
     * Determines if the {@code Graph} implementations supports transactions.
     */
    @Override
    public boolean supportsTransactions() {
        return this.transactionsEnabled;
    }

    /**
     * Determines if the {@code Graph} implementation supports threaded transactions which allow a transaction
     * to be executed across multiple threads via {@link Transaction#createThreadedTx()}.
     */
    @Override
    public boolean supportsThreadedTransactions() {
        return false;
    }

    /**
     * Determines if the {@code Graph} implementations supports read operations as executed with the
     * {@link GraphTraversalSource#io(String)} step. Graph implementations will generally support this by
     * default as any graph that can support direct mutation through the Structure API will by default
     * accept data from the standard TinkerPop {@link GraphReader} implementations. However, some graphs like
     * {@code HadoopGraph} don't accept direct mutations but can still do reads from that {@code io()} step.
     */
    @Override
    public boolean supportsIoRead() {
        return true;
    }

    /**
     * Determines if the {@code Graph} implementations supports write operations as executed with the
     * {@link GraphTraversalSource#io(String)} step. Graph implementations will generally support this by
     * default given the standard TinkerPop {@link GraphWriter} implementations. However, some graphs like
     * {@code HadoopGraph} will use a different approach to handle writes.
     */
    @Override
    public boolean supportsIoWrite() {
        return true;
    }

    /**
     * Determines if the {@code Graph} implementation supports total universal orderability per the Gremlin
     * orderability semantics.
     */
    @Override
    public boolean supportsOrderabilitySemantics() {
        return true;
    }

    /**
     * Determines if the {@code Graph} implementation supports the service call feature.
     */
    @Override
    public boolean supportsServiceCall() {
        return true;
    }

    /**
     * Gets the features related to "graph sideEffects" operation.
     */
    @Override
    public Graph.Features.VariableFeatures variables() {
        return new FireflyVariableFeatures();
    }
}
