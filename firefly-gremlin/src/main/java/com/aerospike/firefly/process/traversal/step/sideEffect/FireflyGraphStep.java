package com.aerospike.firefly.process.traversal.step.sideEffect;

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
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiPredicate;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Pieter Martin
 * @author Grant Haywood <a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder {
    private final List<HasContainer> hasContainers = new ArrayList<>();
    private final List<Iterator> iterators = new ArrayList<>();

    public FireflyGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);
        this.setIteratorSupplier(() -> (Iterator<E>) (Vertex.class.isAssignableFrom(this.returnClass) ? this.vertices() : this.edges()));
    }

    private Iterator<? extends Edge> edges() {
        final FireflyGraph graph = (FireflyGraph) this.getTraversal().getGraph().get();

        // do we have an index over edges
        final HasContainer indexedContainer = getIndexKey(FireflyEdge.class);
        Iterator<? extends Edge> iterator;
        // ids are present, filter on them first
        if (null == this.ids)
            iterator = Collections.emptyIterator();
        else if (this.ids.length > 0)
            iterator = this.iteratorList(graph.edges(this.ids));
        else if (indexedContainer == null || indexedContainer.getKey() == null)
            iterator = this.iteratorList(graph.edges());
        else if (indexedContainer.getKey().startsWith("~label"))
            iterator = this.iteratorList(FireflyHelper.queryEdgeByLabelStringIndex(graph, indexedContainer.getPredicate().getValue()));
        else if (indexedContainer.getKey().startsWith("~"))
            iterator = this.iteratorList(graph.edges());
        else if (indexedContainer.getValue().getClass().isAssignableFrom(String.class))
            iterator = this.iteratorList(FireflyHelper.queryEdgeStringIndex(graph, indexedContainer.getKey(), indexedContainer.getPredicate().getValue()));
        else if (Number.class.isAssignableFrom(indexedContainer.getValue().getClass()))
            iterator = this.iteratorList(FireflyHelper.queryEdgeNumericIndex(graph, indexedContainer.getKey(), indexedContainer.getPredicate()));
        else
            iterator = Collections.emptyIterator();

        iterators.add(iterator);
        return iterator;
    }

    private Iterator<? extends Vertex> vertices() {
        final FireflyGraph graph = (FireflyGraph) this.getTraversal().getGraph().get();
        final HasContainer indexedContainer = getIndexKey(FireflyVertex.class);
        Iterator<? extends Vertex> iterator;
        if (null == this.ids)
            iterator = Collections.emptyIterator();
        else if (this.ids.length > 0)
            iterator = this.iteratorList(graph.vertices(this.ids));
        else if (indexedContainer == null || indexedContainer.getKey() == null)
            iterator = this.iteratorList(graph.vertices());
        else if (indexedContainer.getKey().equals("~label"))
            iterator = this.iteratorList(FireflyHelper.queryVertexByLabelStringIndex(graph, indexedContainer.getPredicate().getValue()));
        else if (indexedContainer.getKey().startsWith("~"))
            iterator = this.iteratorList(graph.vertices());
        else if (indexedContainer.getValue().getClass().isAssignableFrom(String.class) || indexedContainer.getKey().equals("~label"))
            iterator = this.iteratorList(FireflyHelper.queryVertexByVertexPropertyStringIndex(graph, indexedContainer.getKey(), indexedContainer.getPredicate().getValue()));
        else if (Number.class.isAssignableFrom(indexedContainer.getValue().getClass()))
            iterator = this.iteratorList(FireflyHelper.queryVertexByVertexPropertyNumericIndex(graph, indexedContainer.getKey(), indexedContainer.getPredicate()));
        else
            iterator = Collections.emptyIterator();

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
            // we indices for String exact match and Numeric {match,lt,gt} over vertex properties and edge properties
            if (indexedClass.isAssignableFrom(FireflyVertex.class) || indexedClass.isAssignableFrom(FireflyEdge.class)) {
                if (hasContainer == null || hasContainer.getValue() == null)
                    return false;
                if (Long.class.isAssignableFrom(hasContainer.getValue().getClass()) || Integer.class.isAssignableFrom(hasContainer.getValue().getClass()))
                    if (supportedNumericPredicates.contains(hasContainer.getBiPredicate()))
                        return true;
                if (String.class.isAssignableFrom(hasContainer.getValue().getClass()))
                    if (supportedStringPredicates.contains(hasContainer.getBiPredicate()))
                        return true;
            }
            return false; //nothing else
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

    private <E extends Element> Iterator<E> iteratorList(final Iterator<E> iterator) {
        final List<E> list = new ArrayList<>();

        try {
            while (iterator.hasNext()) {
                final E e = iterator.next();
                if (HasContainer.testAll(e, this.hasContainers))
                    list.add(e);
            }
        } finally {
            CloseableIterator.closeIterator(iterator);
        }

        return list.iterator();
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
