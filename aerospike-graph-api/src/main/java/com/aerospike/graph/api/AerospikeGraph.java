package com.aerospike.graph.api;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.service.ServiceRegistry;

import java.util.Iterator;

class AerospikeGraph implements AerospikeGraphApi, Graph {
    private final FireflyGraph graph;

    AerospikeGraph(final FireflyGraph graph) {
        this.graph = graph;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        return this.graph.addVertex(keyValues);
    }

    @Override
    public Vertex addVertex(String label) {
        return this.graph.addVertex(label);
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        return this.graph.compute(graphComputerClass);
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return this.graph.compute();
    }

    @Override
    public <C extends TraversalSource> C traversal(Class<C> traversalSourceClass) {
        return this.graph.traversal(traversalSourceClass);
    }

    @Override
    public GraphTraversalSource traversal() {
        return this.graph.traversal();
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        return this.graph.vertices(vertexIds);
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        return this.graph.edges(edgeIds);
    }

    @Override
    public Transaction tx() {
        return this.graph.tx();
    }

    @Override
    public <Tx extends Transaction> Tx tx(Class<Tx> txClass) {
        return this.graph.tx(txClass);
    }

    @Override
    public void close() {
        this.graph.close();
    }

    @Override
    public <I extends Io> I io(Io.Builder<I> builder) {
        return this.graph.io(builder);
    }

    @Override
    public Variables variables() {
        return this.graph.variables();
    }

    @Override
    public Configuration configuration() {
        return this.graph.configuration();
    }

    @Override
    public ServiceRegistry getServiceRegistry() {
        return this.graph.getServiceRegistry();
    }

    @Override
    public Features features() {
        return this.graph.features();
    }
}
