package com.aerospike.firefly.structure.iterator;

import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class FireflyBatchEdgeIterator<E extends Edge> implements CloseableIterator<E> {
    protected final Iterator<FireflyId> idIterator;
    protected final FireflyGraph graph;
    protected Iterator<FireflyEdge> elementIterator;

    public FireflyBatchEdgeIterator(final FireflyGraph graph, final Iterator<FireflyId> ids) {
        this.idIterator = ids;
        this.graph = graph;
        this.elementIterator = null;
    }

    @Override
    public boolean hasNext() {
        while (true) {
            if (elementIterator == null || !elementIterator.hasNext()) {
                if (!idIterator.hasNext()) {
                    return false;
                }
                final List<FireflyId> fireflyIdList = new ArrayList<>();
                while (idIterator.hasNext() && fireflyIdList.size() < graph.getBaseGraph().getConfig().aerospikeBatchReadSize) {
                    fireflyIdList.add(idIterator.next());
                }
                elementIterator = graph.getAerospikeOperations().readEdges(fireflyIdList).iterator();
            } else { // We still have data to return.
                return true;
            } // Just in case the ids we go to read have been removed we should not straight up return true, loop again
        }
    }

    @Override
    public E next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return (E) elementIterator.next();
    }

    @Override
    public void close() {
        if (elementIterator != null) {
            CloseableIterator.closeIterator(elementIterator);
        }
        if (idIterator != null) {
            CloseableIterator.closeIterator(idIterator);
        }
    }
}
