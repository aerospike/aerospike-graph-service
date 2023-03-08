package com.aerospike.firefly.structure.iterator;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyBatchReadVertexIterator implements Iterator<Vertex> {
    private Iterator<FireflyVertex> vertexIterator;
    private final Iterator<FireflyId> idIterator;
    private final FireflyGraph graph;

    public FireflyBatchReadVertexIterator(final FireflyGraph graph, final Iterator<FireflyId> ids) {
        this.idIterator = ids;
        this.graph = graph;
        this.vertexIterator = null;
    }

    @Override
    public boolean hasNext() {
        if (vertexIterator == null || !vertexIterator.hasNext()) {
            if (!idIterator.hasNext()) {
                return false;
            }
            final List<FireflyId> fireflyIdList = new ArrayList<>();
            while (idIterator.hasNext() && fireflyIdList.size() < graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE) {
                fireflyIdList.add(idIterator.next());
            }
            vertexIterator = graph.readVertices(List.of(), fireflyIdList).iterator();

            // Just in case the ids we go to read have been removed we should not straight up return true.
            return vertexIterator.hasNext();
        }

        // We still have data to return.
        return true;
    }

    @Override
    public Vertex next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return vertexIterator.next();
    }
}
