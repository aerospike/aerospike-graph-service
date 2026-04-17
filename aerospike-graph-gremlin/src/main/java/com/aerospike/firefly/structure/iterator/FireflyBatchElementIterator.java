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

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class FireflyBatchElementIterator<E extends Element, F extends E> implements CloseableIterator<E> {
    private Iterator<F> elementIterator;
    private final Iterator<FireflyId> idIterator;
    private final FireflyGraph graph;
    private final ReadElements<F> readElements;
    private final List<HasContainer> hasContainers;
    private final List<String> requiredProperties;

    public interface ReadElements<G> {
        List<G> readElements(List<HasContainer> hasContainers, List<FireflyId> ids, List<String> requiredProperties);
    }

    public FireflyBatchElementIterator(final FireflyGraph graph, final Iterator<FireflyId> ids, final List<HasContainer> filters, final ReadElements<F> readElements, final List<String> requiredProperties) {
        this.idIterator = ids;
        this.graph = graph;
        this.elementIterator = null;
        this.readElements = readElements;
        this.hasContainers = filters;
        this.requiredProperties = requiredProperties;
    }

    @Override
    public boolean hasNext() {
        while (true) {
            if (elementIterator == null || !elementIterator.hasNext()) {
                if (!idIterator.hasNext()) {
                    return false;
                }
                final List<FireflyId> fireflyIdList = new ArrayList<>(graph.getBaseGraph().getConfig().aerospikeBatchReadSize);
                while (idIterator.hasNext() && fireflyIdList.size() < graph.getBaseGraph().getConfig().aerospikeBatchReadSize) {
                    fireflyIdList.add(idIterator.next());
                }
                elementIterator = readElements.readElements(hasContainers, fireflyIdList, requiredProperties).iterator();
            } else {
                // We still have data to return.
                return true;
            }
            // If the batch returned empty (all elements deleted), loop to try the next batch.
        }
    }

    @Override
    public E next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return elementIterator.next();
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
