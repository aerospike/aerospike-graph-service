package com.aerospike.firefly.process.traversal.step.sideEffect;

import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.impl.SubgraphPrefetchTask;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
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
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Pieter Martin
 * @author Grant Haywood <a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyGraphStep.class);
    private final List<HasContainer> hasContainers = new ArrayList<>();
    private final List<Iterator> iterators = new ArrayList<>();

    public FireflyGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

    /**
     * Get an iterator of all the edges, with filter if filter is applied
     * if index is available for the filter, use the index to fill the iterator
     *
     * @return iterator of edges
     */
    private Iterator<? extends Edge> edges() {
        final FireflyGraph graph = (FireflyGraph) this.getTraversal().getGraph().get();

        // do we have an index over edges
        final HasContainer indexedContainer = getIndexKey(FireflyEdge.class);
        Iterator<? extends Edge> iterator;
        // ids are present, filter on them first
        if (null == this.ids)
            iterator = Collections.emptyIterator();
        else if (this.ids.length > 0)
            iterator = this.hasContainerCheckedIterator(graph.edges(this.ids));
        else if (indexedContainer == null || indexedContainer.getKey() == null)
            iterator = this.hasContainerCheckedIterator(graph.edges());
        else if (indexedContainer.getKey().startsWith("~label"))
            iterator = this.hasContainerCheckedIterator(FireflyHelper.queryEdgeByLabelStringIndex(graph, indexedContainer.getPredicate().getValue()));
        else if (indexedContainer.getKey().startsWith("~"))
            iterator = this.hasContainerCheckedIterator(graph.edges());
        else if (indexedContainer.getValue().getClass().isAssignableFrom(String.class))
            iterator = this.hasContainerCheckedIterator(FireflyHelper.queryEdgeStringIndex(graph, indexedContainer.getKey(), indexedContainer.getPredicate().getValue()));
        else if (Number.class.isAssignableFrom(indexedContainer.getValue().getClass()))
            iterator = this.hasContainerCheckedIterator(FireflyHelper.queryEdgeNumericIndex(graph, indexedContainer.getKey(), indexedContainer.getPredicate()));
        else
            iterator = Collections.emptyIterator();

        iterators.add(iterator);
        return iterator;
    }

    /**
     * Get an iterator of all the vertices, with filter if filter is applied
     * if index is available for the filter, use the index to fill the iterator
     *
     * @return iterator of vertices
     */
    private Iterator<? extends Vertex> vertices() {
        final FireflyGraph graph = (FireflyGraph) this.getTraversal().getGraph().get();
        final HasContainer indexedContainer = getIndexKey(FireflyVertex.class);
        Iterator<? extends Vertex> iterator;
        if (null == this.ids) {
            iterator = Collections.emptyIterator();
        } else if (this.ids.length > 0) {
            iterator = graph.vertices(this.ids);
        } else if (indexedContainer == null || indexedContainer.getKey() == null ||
                (indexedContainer.getKey().startsWith("~") && !indexedContainer.getKey().equals("~label"))) {
            // If index container is null or key is null or if key starts with ~ but is not ~label, then get graph.vertices().
            iterator = graph.vertices();
        } else if (indexedContainer.getKey().equals("~label") ||
                Number.class.isAssignableFrom(indexedContainer.getValue().getClass()) ||
                String.class.isAssignableFrom(indexedContainer.getValue().getClass())) {
            // Find index.
            final Optional<FireflyIndexMetadata.IndexInfo> propertyIndexInfo =
                    graph.fireflyIndexMetadata.getPropertyIndexInfo(indexedContainer.getKey(), indexedContainer.getValue());

            // If we have index, query it, otherwise we need to scan (or error out).
            if (propertyIndexInfo.isPresent()) {
                iterator = graph.queryIndex(propertyIndexInfo.get(), indexedContainer.getPredicate(), graph::vertexFromRecord);
            } else {
                LOG.debug("No index found for key {} and value {}, running scan", indexedContainer.getKey(), indexedContainer.getValue());
                iterator = graph.queryScan(indexedContainer.getKey(), indexedContainer.getPredicate(), graph::vertexFromRecord);
            }
        } else {
            iterator = Collections.emptyIterator();
        }
        // Need to wrap iterator in hasContainerCheckedIterator() to apply hasContainers.
        iterator = this.hasContainerCheckedIterator(iterator);
        iterators.add(iterator);
        return iterator;
    }

    private HasContainer getIndexKey(final Class<? extends FireflyElement> indexedClass) {
        final ArrayList<BiPredicate> supportedNumericPredicates = new ArrayList<>() {{
            add(Compare.eq);
            add(Compare.lt);
            add(Compare.gt);
        }};
        final ArrayList<BiPredicate> supportedStringPredicates = new ArrayList<>() {{
            add(Compare.eq);
        }};
        final Iterator<HasContainer> itty = IteratorUtils.filter(hasContainers.iterator(), hasContainer -> {
            // we have indices for String exact match and Numeric {match,lt,gt} over vertex properties and edge properties
            if (indexedClass.isAssignableFrom(FireflyVertex.class) || indexedClass.isAssignableFrom(FireflyEdge.class)) {
                if (hasContainer == null || hasContainer.getValue() == null) {
                    return false;
                } else if (Long.class.isAssignableFrom(hasContainer.getValue().getClass()) || Integer.class.isAssignableFrom(hasContainer.getValue().getClass())) {
                    return supportedNumericPredicates.contains(hasContainer.getBiPredicate());
                } else if (String.class.isAssignableFrom(hasContainer.getValue().getClass())) {
                    return supportedStringPredicates.contains(hasContainer.getBiPredicate());
                }
            }
            // No other cases.
            return false;
        });
        return itty.hasNext() ? itty.next() : null;
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

    private <E extends Element> Iterator<E> hasContainerCheckedIterator(final Iterator<E> iterator) {
        return new HasContainerIterator<>(iterator, this.hasContainers);
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
