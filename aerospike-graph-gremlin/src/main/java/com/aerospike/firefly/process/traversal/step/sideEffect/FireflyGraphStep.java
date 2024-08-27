package com.aerospike.firefly.process.traversal.step.sideEffect;

import com.aerospike.firefly.io.FireflyCardinalityMetadata;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.query.GraphQuery;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.TimeoutHelper;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.AndP;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Pieter Martin
 * @author Grant Haywood <a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyGraphStep.class);
    private final List<HasContainer> hasContainers = new ArrayList<>();
    private final List<Iterator> iterators = new ArrayList<>();
    private List<String> properties = null;
    private Long evaluationTimeout;

    public FireflyGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);
        this.setIteratorSupplier(() -> (Vertex.class.isAssignableFrom(this.returnClass) ? (Iterator<E>) this.vertices() : (Iterator<E>) this.edges()));
        this.evaluationTimeout = TimeoutHelper.calculate(originalGraphStep.getTraversal());
    }

    public void addProperties(final List<String> properties) {
        this.properties = new ArrayList<>(properties);
    }

    /**
     * Get an iterator of all the edges, with filter if filter is applied
     * if index is available for the filter, use the index to fill the iterator
     *
     * @return iterator of edges
     */
    private Iterator<? extends Edge> edges() {
        // Get FireflyGraph from the traversal.
        final FireflyGraph graph = (FireflyGraph) this.getTraversal().getGraph().get();

        // Grab all Edges for iterator.
        final Iterator<? extends Edge> iterator = phatEdges(graph);

        // Return base iterator.
        return iterator;
    }

    /**
     * Get an iterator of all the vertices, with filter if filter is applied
     * if index is available for the filter, use the index to fill the iterator
     *
     * @return iterator of vertices
     */
    private Iterator<? extends Vertex> vertices() {
        // Get FireflyGraph from the traversal.
        final FireflyGraph graph = (FireflyGraph) this.getTraversal().getGraph().get();

        // Grab all Vertices for iterator.
        final Iterator<? extends Vertex> iterator = vertices(
                graph,
                graph.getBaseGraph().VERTEX_AERO_SET,
                graph.getBaseGraph().VERTEX_PROPERTY_NAME_TO_VALUE_BIN,
                FireflyVertex.class,
                new FireflyGraph.GetElements<Vertex>() {
                    @Override
                    public Iterator<Vertex> getFiltered(final List<HasContainer> hasContainers, final Object... ids) {
                        return graph.vertices(hasContainers, properties, ids);
                    }

                    @Override
                    public Iterator<Vertex> getUnfiltered(final Object... ids) {
                        return graph.vertices(properties, ids);
                    }
                },
                graph::vertexFromRecord);

        // Return base iterator.
        return iterator;
    }

    /**
     * This private helper function is no longer used for anything but vertices, but can be lightly modified to restore
     * its original function of being used for any scalar record element types if needed in the future.
     */
    private <R extends Element> Iterator<R> vertices(final FireflyGraph graph,
                                                     final String setName,
                                                     final String binName,
                                                     final Class<? extends FireflyElement> elementClass,
                                                     final FireflyGraph.GetElements<R> getElements,
                                                     final FireflyGraph.TransformKeyRecord<R> transformKeyRecord) {
        Iterator<R> iterator;
        final List<HasContainerWithCardinality> sortedHasContainers = FireflyBatchReadHelper.getHasContainersWithCardinalityOrder(graph, returnClass, hasContainers);
        final List<HasContainer> aerospikeSideHasContainers = FireflyBatchReadHelper.getAerospikeHasContainers(sortedHasContainers);
        // TODO GRAPH-401: This is a hack to get around the fact that we cannot filter our cache with a hasContainer.
        //  To get around this we have to filter everything post read again, so all containers pushed to firefly no
        //  matter what.
        final List<HasContainer> fireflySideHasContainers = sortedHasContainers.stream().map(a -> a.hasContainer).collect(Collectors.toList());

        if (null == this.ids) {
            iterator = Collections.emptyIterator();
            iterators.add(iterator);
            return iterator;
        } else if (this.ids.length > 0) {
            iterator = aerospikeSideHasContainers.isEmpty() ?
                    getElements.getUnfiltered(this.ids) :
                    getElements.getFiltered(aerospikeSideHasContainers, this.ids);
            iterator = this.hasContainerCheckedIterator(iterator, fireflySideHasContainers);
            iterators.add(iterator);
            return iterator;
        }

        // If there are HasContainers that can be pushed to aerospike, grab first item (this is highest cardinality based on sorting).
        final HasContainer topContainer = aerospikeSideHasContainers.isEmpty() ? null : aerospikeSideHasContainers.get(0);

        if (topContainer == null || topContainer.getKey() == null ||
                (topContainer.getKey().startsWith("~") && !topContainer.getKey().equals("~label"))) {
            // If index container is null or key is null or if key starts with ~ but is not ~label, then get graph.vertices().
            iterator = getElements.getUnfiltered();
            iterator = this.hasContainerCheckedIterator(iterator, hasContainers);
            iterators.add(iterator);
            return iterator;
        } else if (topContainer.getKey().equals("~label") ||
                Number.class.isAssignableFrom(topContainer.getValue().getClass()) ||
                String.class.isAssignableFrom(topContainer.getValue().getClass())) {
            if (aerospikeSideHasContainers.size() > 0) {
                // Don't want to filter on something we run as our primary discriminator.
                aerospikeSideHasContainers.remove(0);
            }

            // Find index.
            final Optional<FireflyIndexMetadata.IndexInfo> propertyIndexInfo =
                    graph.fireflyIndexMetadata.getPropertyIndexInfo(elementClass, topContainer.getKey(), topContainer.getValue());

            // If we have index, query it, otherwise we need to scan (or error out).
            if (propertyIndexInfo.isPresent()) {
                iterator = GraphQuery.create(graph).queryVertexSIndex(propertyIndexInfo.get(),
                        topContainer.getPredicate(),
                        transformKeyRecord,
                        aerospikeSideHasContainers,
                        evaluationTimeout);
            } else {
                LOG.debug("No index found for key {} and value {}, running scan", topContainer.getKey(), topContainer.getValue());
                iterator = GraphQuery.create(graph).scanSet(
                        topContainer.getKey(),
                        setName,
                        topContainer.getKey().equals("~label") ? graph.getBaseGraph().LABEL_BIN : binName,
                        topContainer.getPredicate(),
                        transformKeyRecord,
                        aerospikeSideHasContainers,
                        elementClass,
                        true,
                        true,
                        evaluationTimeout);
            }
            // Need to wrap iterator in hasContainerCheckedIterator() to apply hasContainers that could not be pushed down to Aerospike.
            iterator = this.hasContainerCheckedIterator(iterator, fireflySideHasContainers);
            iterators.add(iterator);
            return iterator;
        } else {
            iterator = Collections.emptyIterator();
            iterators.add(iterator);
            return iterator;
        }
    }

    private <R extends Element> Iterator<R> phatEdges(final FireflyGraph graph) {
        Iterator<R> iterator;
        final List<HasContainerWithCardinality> sortedHasContainers = FireflyBatchReadHelper.getHasContainersWithCardinalityOrder(graph, returnClass, hasContainers);
        final List<HasContainer> fireflySideHasContainers = sortedHasContainers.stream().map(hasContainerWithCardinality -> hasContainerWithCardinality.hasContainer).collect(Collectors.toList());

        if (null == this.ids) {
            iterator = Collections.emptyIterator();
        } else {
            iterator = (Iterator<R>) graph.edges(this.ids);
            iterator = this.hasContainerCheckedIterator(iterator, fireflySideHasContainers);
        }
        iterators.add(iterator);
        return iterator;
    }

    /**
     * Simple class to contain info about has containers.
     * Public only for testing purposes.
     */
    public static class HasContainerWithCardinality {
        public final HasContainer hasContainer;
        public final boolean isLabel;
        public final boolean isSupported;
        public final FireflyCardinalityMetadata.CardinalityInfo cardinality;

        /**
         * Constructor.
         *
         * @param hasContainer The HasContainer.
         * @param cardinality  The cardinality info.
         */
        public HasContainerWithCardinality(final HasContainer hasContainer, final FireflyCardinalityMetadata.CardinalityInfo cardinality) {
            this.hasContainer = hasContainer;
            this.cardinality = cardinality;
            this.isLabel = "~label".equals(hasContainer.getKey());
            this.isSupported = true;
        }

        /**
         * Constructor.
         *
         * @param hasContainer The HasContainer.
         * @param isSupported  true if the hasContainer has a supported predicate.
         */
        public HasContainerWithCardinality(final HasContainer hasContainer, final boolean isSupported) {
            this.hasContainer = hasContainer;
            this.cardinality = null;
            this.isLabel = "~label".equals(hasContainer.getKey());
            this.isSupported = isSupported;
        }

        /**
         * Constructor.
         *
         * @param hasContainer The HasContainer.
         */
        public HasContainerWithCardinality(final HasContainer hasContainer) {
            this.hasContainer = hasContainer;
            this.cardinality = null;
            this.isLabel = "~label".equals(hasContainer.getKey());
            this.isSupported = true;
        }
    }

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty())
            return super.toString();
        else
            return (null == this.ids || 0 == this.ids.length) ?
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.hasContainers) :
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), Arrays.toString(this.ids), this.hasContainers);
    }

    static class HasContainerIterator<E extends Element> implements Iterator<E>, AutoCloseable {

        private final Iterator<E> i;
        private final List<HasContainer> hasContainers;
        private E e;
        private boolean valid;

        public HasContainerIterator(final Iterator<E> iterator, final List<HasContainer> hasContainers) {
            this.i = iterator;
            this.hasContainers = hasContainers;
            this.e = null;
            this.valid = false;
        }

        @Override
        public boolean hasNext() {
            // Element found and is waiting to be grabbed.
            if (valid) {
                return true;
            }

            // Find next element that matches HasContainer.
            while (i.hasNext()) {
                e = i.next();
                if (HasContainer.testAll(e, this.hasContainers)) {
                    valid = true;
                    return true;
                }
            }
            valid = false;
            return false;
        }

        @Override
        public E next() {
            // If they checked hasNext() prior to the next() call and there was an element available, valid will be true.
            if (valid) {
                valid = false;
                return e;
            }

            if (hasNext()) {
                valid = false;
                return e;
            }

            // No more elements available.
            throw FastNoSuchElementException.instance();
        }

        @Override
        public void close() {
            CloseableIterator.closeIterator(i);
        }
    }

    static class HasContainerIteratorIterator<E extends Element> implements Iterator<E>, AutoCloseable {
        private final Iterator<Iterator<E>> i;
        private final List<HasContainer> hasContainers;
        private HasContainerIterator<E> currentIterator;

        public HasContainerIteratorIterator(final Iterator<Iterator<E>> iterator, final List<HasContainer> hasContainers) {
            this.i = iterator;
            this.hasContainers = hasContainers;
            if (iterator.hasNext()) {
                this.currentIterator = new HasContainerIterator<>(iterator.next(), hasContainers);
            } else {
                this.currentIterator = new HasContainerIterator<>(Collections.emptyIterator(), hasContainers);
            }
        }

        @Override
        public boolean hasNext() {
            if (this.currentIterator.hasNext()) {
                return true;
            } else {
                if (this.i.hasNext()) {
                    this.currentIterator = new HasContainerIterator<>(this.i.next(), this.hasContainers);
                    return this.hasNext();
                } else {
                    return false;
                }
            }
        }

        @Override
        public E next() {
            if (this.hasNext()) {
                return this.currentIterator.next();
            } else {
                throw FastNoSuchElementException.instance();
            }
        }

        @Override
        public void close() {
            CloseableIterator.closeIterator(this.currentIterator);
            while (this.i.hasNext()) {
                CloseableIterator.closeIterator(this.i.next());
            }
        }
    }

    private <E extends Element> Iterator<E> hasContainerCheckedIterator(final Iterator<E> iterator, final List<HasContainer> minimalHasContainers) {
        return new HasContainerIterator<>(iterator, minimalHasContainers);
    }

    private <E extends Element> Iterator<E> hasContainerCheckedIteratorIterator(final Iterator<Iterator<E>> iterator, final List<HasContainer> minimalHasContainers) {
        return new HasContainerIteratorIterator<>(iterator, minimalHasContainers);
    }

    @Override
    public List<HasContainer> getHasContainers() {
        return Collections.unmodifiableList(this.hasContainers);
    }

    @Override
    public void addHasContainer(HasContainer hasContainer) {
        if (hasContainer.getPredicate() instanceof AndP) {
            for (final P<?> predicate : ((AndP<?>) hasContainer.getPredicate()).getPredicates()) {
                this.addHasContainer(new HasContainer(hasContainer.getKey(), predicate));
            }
        } else
            this.hasContainers.add(hasContainer);
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.hasContainers.hashCode();
    }

    @Override
    public void close() {
        iterators.forEach(CloseableIterator::closeIterator);
    }
}
