package com.aerospike.firefly;

import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.event.MutationListener;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategy;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestConcurrentScanListener extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    static class StubMutationListener implements MutationListener {
        private final AtomicLong addEdgeEvent = new AtomicLong(0);
        private final AtomicLong addVertexEvent = new AtomicLong(0);
        private final AtomicLong vertexRemovedEvent = new AtomicLong(0);
        private final AtomicLong edgePropertyChangedEvent = new AtomicLong(0);
        private final AtomicLong vertexPropertyChangedEvent = new AtomicLong(0);
        private final AtomicLong vertexPropertyPropertyChangedEvent = new AtomicLong(0);
        private final AtomicLong edgePropertyRemovedEvent = new AtomicLong(0);
        private final AtomicLong vertexPropertyPropertyRemovedEvent = new AtomicLong(0);
        private final AtomicLong edgeRemovedEvent = new AtomicLong(0);
        private final AtomicLong vertexPropertyRemovedEvent = new AtomicLong(0);

        private final ConcurrentLinkedQueue<String> order = new ConcurrentLinkedQueue<>();

        public void reset() {
            addEdgeEvent.set(0);
            addVertexEvent.set(0);
            vertexRemovedEvent.set(0);
            edgePropertyChangedEvent.set(0);
            vertexPropertyChangedEvent.set(0);
            vertexPropertyPropertyChangedEvent.set(0);
            vertexPropertyPropertyRemovedEvent.set(0);
            edgePropertyRemovedEvent.set(0);
            edgeRemovedEvent.set(0);
            vertexPropertyRemovedEvent.set(0);

            order.clear();
        }

        public List<String> getOrder() {
            return new ArrayList<>(this.order);
        }

        @Override
        public void vertexAdded(final Vertex vertex) {
            addVertexEvent.incrementAndGet();
            order.add("v-added-" + vertex.id());
        }

        @Override
        public void vertexRemoved(final Vertex vertex) {
            vertexRemovedEvent.incrementAndGet();
            order.add("v-removed-" + vertex.id());
        }

        @Override
        public void edgeAdded(final Edge edge) {
            addEdgeEvent.incrementAndGet();
            order.add("e-added-" + edge.id());
        }

        @Override
        public void edgePropertyRemoved(final Edge element, final Property o) {
            edgePropertyRemovedEvent.incrementAndGet();
            order.add("e-property-removed-" + element.id() + "-" + o);
        }

        @Override
        public void vertexPropertyPropertyRemoved(final VertexProperty element, final Property o) {
            vertexPropertyPropertyRemovedEvent.incrementAndGet();
            order.add("vp-property-removed-" + element.id() + "-" + o);
        }

        @Override
        public void edgeRemoved(final Edge edge) {
            edgeRemovedEvent.incrementAndGet();
            order.add("e-removed-" + edge.id());
        }

        @Override
        public void vertexPropertyRemoved(final VertexProperty vertexProperty) {
            vertexPropertyRemovedEvent.incrementAndGet();
            order.add("vp-property-removed-" + vertexProperty.id());
        }

        @Override
        public void edgePropertyChanged(final Edge element, final Property oldValue, final Object setValue) {
            edgePropertyChangedEvent.incrementAndGet();
            order.add("e-property-chanaged-" + element.id());
        }

        @Override
        public void vertexPropertyPropertyChanged(final VertexProperty element, final Property oldValue, final Object setValue) {
            vertexPropertyPropertyChangedEvent.incrementAndGet();
            order.add("vp-property-changed-" + element.id());
        }

        @Override
        public void vertexPropertyChanged(final Vertex element, final VertexProperty oldValue, final Object setValue, final Object... vertexPropertyKeyValues) {
            vertexPropertyChangedEvent.incrementAndGet();
            order.add("v-property-changed-" + element.id());
        }

        public long addEdgeEventRecorded() {
            return addEdgeEvent.get();
        }

        public long addVertexEventRecorded() {
            return addVertexEvent.get();
        }

        public long vertexRemovedEventRecorded() {
            return vertexRemovedEvent.get();
        }

        public long edgeRemovedEventRecorded() {
            return edgeRemovedEvent.get();
        }

        public long edgePropertyRemovedEventRecorded() {
            return edgePropertyRemovedEvent.get();
        }

        public long vertexPropertyRemovedEventRecorded() {
            return vertexPropertyRemovedEvent.get();
        }

        public long vertexPropertyPropertyRemovedEventRecorded() {
            return vertexPropertyPropertyRemovedEvent.get();
        }

        public long edgePropertyChangedEventRecorded() {
            return edgePropertyChangedEvent.get();
        }

        public long vertexPropertyChangedEventRecorded() {
            return vertexPropertyChangedEvent.get();
        }

        public long vertexPropertyPropertyChangedEventRecorded() {
            return vertexPropertyPropertyChangedEvent.get();
        }
    }
    private GraphTraversalSource create(final EventStrategy strategy) {
        return this.graph.traversal().withStrategies(strategy);
    }
    public void tryCommit(final Graph graph, final Consumer<Graph> assertFunction) {
        assertFunction.accept(graph);
        if (graph.features().graph().supportsTransactions()) {
            graph.tx().commit();
            assertFunction.accept(graph);
        }
    }
}
