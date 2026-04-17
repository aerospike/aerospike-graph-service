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

package com.aerospike.firefly.olap.structure;

import org.apache.commons.collections4.MultiValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.IndexedTraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Host;

import java.io.Serializable;
import java.util.Collection;
import java.util.function.Function;

// Would be nice to fix the tinkerpop VertexIndexTraverserSet so that we don't need this class.
/**
 * Most of the Distributed* classes are adapted from the Spark* in TinkerPop from by Marko A. Rodriguez (http://markorodriguez.com)
 */
public class DistributedIndexedTraverserSet <S,I> extends TraverserSet<S> implements Serializable {
    final MultiValuedMap<I, Traverser.Admin<S>> index = new ArrayListValuedHashMap<>();
    final SerializableFunction<S,I> indexingFunction;

    public DistributedIndexedTraverserSet(IndexedTraverserSet.VertexIndexedTraverserSet thisReallyStupidObjectIHate) {
        this((SerializableFunction) DistributedIndexedTraverserSet::getHostingVertex, null);
    }

    public DistributedIndexedTraverserSet(final SerializableFunction<S, I> indexingFunction, final Traverser.Admin<S> traverser) {
        super(traverser);
        this.indexingFunction = indexingFunction;
    }

    interface SerializableFunction<T, R> extends Function<T, R>, Serializable {
    }

    public static Vertex getHostingVertex(final Object object) {
        Object obj = object;
        while (true) {
            if (obj instanceof Vertex)
                return (Vertex) obj;
            else if (obj instanceof Edge)
                return ((Edge) obj).outVertex();
            else if (obj instanceof Property)
                obj = ((Property) obj).element();
            else
                throw new IllegalStateException("The host of the object is unknown: " + obj.toString() + ':' + obj.getClass().getCanonicalName());
        }
    }

    @Override
    public void clear() {
        index.clear();
        super.clear();
    }

    @Override
    public boolean add(final Traverser.Admin<S> traverser) {
        final boolean newOne = super.add(traverser);

        // if newly added then the traverser will be the same as the one passed in here to add().
        // if it is not, then it was merged to an existing traverser and the bulk would have
        // updated on that reference, thus only new stuff really needs to be added to the index
        if (newOne) index.put(indexingFunction.apply(traverser.get()), traverser);

        return newOne;
    }

    /**
     * Gets a collection of {@link Traverser} objects that contain the specified value.
     *
     * @param k the key produced by the indexing function
     * @return
     */
    public Collection<Traverser.Admin<S>> get(final I k) {
        final Collection<Traverser.Admin<S>> c = index.get(k);

        // if remove() is called on this class, then the MultiValueMap *may* (javadoc wasn't clear
        // what the expectation was - used the word "typically") return an empty list if the last
        // item removed leaves the list empty. i think we want to enforce null for TraverserSet
        // semantics
        return c != null && c.isEmpty() ? null : c;
    }

    @Override
    public boolean offer(final Traverser.Admin<S> traverser) {
        return this.add(traverser);
    }

    @Override
    public Traverser.Admin<S> remove() {
        final Traverser.Admin<S> removed = super.remove();
        index.removeMapping(indexingFunction.apply(removed.get()), removed);
        return removed;
    }

    @Override
    public boolean remove(final Object traverser) {
        if (!(traverser instanceof Traverser.Admin))
            throw new IllegalArgumentException("The object to remove must be traverser");

        final boolean removed = super.remove(traverser);
        if (removed) index.removeMapping(indexingFunction.apply(((Traverser.Admin<S>) traverser).get()), traverser);
        return removed;
    }

    /**
     * An {@link IndexedTraverserSet} that indexes based on a {@link Vertex} traverser.
     */
    public static class VertexIndexedTraverserSet extends IndexedTraverserSet<Object, Vertex> {
        public VertexIndexedTraverserSet() {
            super(Host::getHostingVertex);
        }
    }
}
