/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
