package com.aerospike.firefly.structure.iterator;

import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Edge;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * Used for FireflyMergeEdge step, which filters Adjacent Vertex ID and Label beforehand.
 *
 */
public class FireflyFilteredBatchEdgeIterator<E extends Edge> extends FireflyBatchEdgeIterator<E> {
    private final List<HasContainer> filters;
    private E next = null;

    public FireflyFilteredBatchEdgeIterator(final FireflyGraph graph, final Iterator<FireflyId> ids,
                                            final List<HasContainer> filters) {
        super(graph, ids);
        this.filters = filters;
    }

    @Override
    public boolean hasNext() {
        if (next != null) {
            // We're holding a valid element to return.
            return true;
        }
        if (elementIterator == null || !elementIterator.hasNext()) {
            // The current element iterator has been exhausted. Grab more elements using the ID iterator.
            if (!idIterator.hasNext()) {
                return false;
            }
            final List<FireflyId> fireflyIdList = new ArrayList<>();
            while (idIterator.hasNext() && fireflyIdList.size() < graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE) {
                fireflyIdList.add(idIterator.next());
            }
            elementIterator = graph.getOperations().readEdges(fireflyIdList).iterator();
            return hasNext();
        } else {
            // Search the current element iterator for one that passes the filters.
            while (elementIterator.hasNext()) {
                final E edge = (E) elementIterator.next();
                if (HasContainer.testAll(edge, filters)) {
                    // We found an element. Break and return.
                    next = edge;
                    return true;
                }
            }
            // A valid element could not be found using the current iterator. Start again.
            return hasNext();
        }
    }

    @Override
    public E next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        final E toReturn = next;
        next = null;
        return toReturn;
    }
}
