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

package com.aerospike.firefly.process.traversal.step.sideEffect;

import com.aerospike.firefly.io.FireflyCardinalityMetadata;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.process.traversal.predicate.GeoPredicate;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.structure.FireflyEdge;
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

import static com.aerospike.firefly.process.traversal.step.util.TraversalUtil.fireflyTestAll;

public class FireflyGraphStep<S, E extends Element> extends GraphStep<S, E> implements HasContainerHolder {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyGraphStep.class);
    private final List<HasContainer> hasContainers = new ArrayList<>();
    private final List<Iterator> iterators = new ArrayList<>();
    private List<String> properties = null;
    private final Long evaluationTimeout;
    private RunType runType;

    enum RunType {
        UNKNOWN,
        PI,
        SI,
        SCAN
    }

    public FireflyGraphStep(final GraphStep<S, E> originalGraphStep) {
        super(originalGraphStep.getTraversal(), originalGraphStep.getReturnClass(), originalGraphStep.isStartStep(), originalGraphStep.getIds());
        originalGraphStep.getLabels().forEach(this::addLabel);
        this.evaluationTimeout = TimeoutHelper.calculate(originalGraphStep.getTraversal());
        this.setIteratorSupplier(() -> (Vertex.class.isAssignableFrom(this.returnClass) ? (Iterator<E>) this.vertices() : (Iterator<E>) this.edges()));
        findRunType();
    }

    private void findRunType() {
        // If these values are stored they end up giving strange errors in some traversals later, so this is done here and repeated below.
        final FireflyGraph graph = (FireflyGraph) this.getTraversal().getGraph().get();
        final List<HasContainerWithCardinality> sortedHasContainers = FireflyBatchReadHelper.getHasContainersWithCardinalityOrder(graph, this.returnClass, hasContainers);
        final List<HasContainer> aerospikeSideHasContainers = FireflyBatchReadHelper.getAerospikeHasContainers(sortedHasContainers);

        if (ids != null && ids.length > 0) {
            runType = RunType.PI;
            return;
        }
        final HasContainer topContainer = aerospikeSideHasContainers.isEmpty() ? null : aerospikeSideHasContainers.get(0);
        final Optional<FireflyIndexMetadata.IndexInfo> propertyIndexInfo;
        if (topContainer != null && topContainer.getKey() != null) {
            propertyIndexInfo = graph.fireflyIndexMetadata.getPropertyIndexInfo(Vertex.class.isAssignableFrom(this.returnClass) ?
                    FireflyVertex.class : FireflyEdge.class, topContainer.getKey(), topContainer.getValue());
        } else {
            propertyIndexInfo = Optional.empty();
        }
        if (topContainer == null || topContainer.getKey() == null ||
                (topContainer.getKey().startsWith("~") && !topContainer.getKey().equals("~label"))) {
            runType = RunType.SCAN;
        } else if (topContainer.getKey().equals("~label") ||
                Number.class.isAssignableFrom(topContainer.getValue().getClass()) ||
                String.class.isAssignableFrom(topContainer.getValue().getClass())) {
            // If we have index, query it, otherwise we need to scan (or error out).
            runType = propertyIndexInfo.isPresent() ? RunType.SI : RunType.SCAN;
        } else {
            runType = RunType.UNKNOWN;
        }
    }

    public void addProperties(final List<String> properties) {
        this.properties = new ArrayList<>(properties);
    }

    public List<String> getProperties() {
        return this.properties;
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
        final Iterator<Vertex> iterator = searchVerticesOptimally(graph, this.hasContainers, this.properties, this.evaluationTimeout, this.ids);

        // Return base iterator.
        this.iterators.add(iterator);
        return iterator;
    }

    public static Iterator<Vertex> searchVerticesOptimally(final FireflyGraph graph,
                                                           final List<HasContainer> hasContainers,
                                                           final List<String> requiredProperties,
                                                           final long evaluationTimeout,
                                                           final Object... vertexIds) {
        // TODO GRAPH-401: post-read filter uses the full container list; server-side filters (expression
        //  filters, index scans) do not universally match fireflyTestAll semantics (missing bins,
        //  unsupported types), so every predicate is still re-checked client-side.
        final FireflyBatchReadHelper.SplitHasContainers split = FireflyBatchReadHelper.splitHasContainers(
                graph, FireflyVertex.class, hasContainers);
        final List<HasContainer> aerospikeSideHasContainers = new ArrayList<>(split.aerospike);
        final List<HasContainer> fireflySideHasContainers = split.all;
        Iterator<Vertex> iterator;

        if (vertexIds == null) {
            return Collections.emptyIterator();
        } else if (vertexIds.length > 0) {
            iterator = graph.vertices(aerospikeSideHasContainers, requiredProperties, vertexIds);
            iterator = hasContainerCheckedIterator(iterator, fireflySideHasContainers);
            return iterator;
        }

        final Optional<FireflyIndexMetadata.ExpressionIndexInfo> expressionIndexInfo =
                graph.fireflyIndexMetadata.getMatchingExpressionIndex(aerospikeSideHasContainers);
        if (expressionIndexInfo.isPresent()) {
            iterator = graph.graphQuery.queryVertexExpressionIndex(expressionIndexInfo.get(), graph::vertexFromRecord,
                    evaluationTimeout);
            iterator = hasContainerCheckedIterator(iterator, fireflySideHasContainers);
            return iterator;
        }

        // If there are HasContainers that can be pushed to Aerospike, grab the first item (this is the highest cardinality based on sorting).
        final HasContainer topContainer = aerospikeSideHasContainers.isEmpty() ? null : aerospikeSideHasContainers.get(0);

        if (topContainer == null || topContainer.getKey() == null ||
                (topContainer.getKey().startsWith("~") && !topContainer.getKey().equals("~label"))) {
            iterator = graph.vertices();
            iterator = hasContainerCheckedIterator(iterator, fireflySideHasContainers);
            return iterator;
        } else if (topContainer.getKey().equals("~label") ||
                Number.class.isAssignableFrom(topContainer.getValue().getClass()) ||
                String.class.isAssignableFrom(topContainer.getValue().getClass()) ||
                GeoPredicate.unwrap(topContainer.getPredicate()) != null) {
            if (aerospikeSideHasContainers.size() > 0) {
                // Don't want to filter on something we run as our primary discriminator.
                aerospikeSideHasContainers.remove(0);
            }

            // Find index.
            final Object indexLookupValue = GeoPredicate.unwrap(topContainer.getPredicate()) != null
                    ? topContainer.getPredicate().getValue()
                    : topContainer.getValue();
            final Optional<FireflyIndexMetadata.IndexInfo> propertyIndexInfo =
                    graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, topContainer.getKey(), indexLookupValue);

            // If we have index, query it, otherwise we need to scan (or error out).
            if (propertyIndexInfo.isPresent()) {
                iterator = graph.graphQuery.queryVertexSIndex(propertyIndexInfo.get(),
                        topContainer.getPredicate(),
                        graph::vertexFromRecord,
                        aerospikeSideHasContainers,
                        evaluationTimeout);
            } else {
                LOG.debug("No index found for key {} and value {}, running scan", topContainer.getKey(), topContainer.getValue());
                final GeoPredicate geoPredicate = GeoPredicate.unwrap(topContainer.getPredicate());
                final String scanBin = geoPredicate != null
                        ? graph.getBaseGraph().getConfig().geoDataBin
                        : (topContainer.getKey().equals("~label")
                        ? graph.getBaseGraph().getConfig().labelBin
                        : graph.getBaseGraph().getConfig().vertexPropertyDataBin);
                iterator = graph.graphQuery.scanSet(
                        topContainer.getKey(),
                        graph.getBaseGraph().getConfig().vertexAeroSet,
                        scanBin,
                        geoPredicate == null ? topContainer.getPredicate() : null,
                        graph::vertexFromRecord,
                        aerospikeSideHasContainers,
                        FireflyVertex.class,
                        true,
                        evaluationTimeout);
            }
            iterator = hasContainerCheckedIterator(iterator, fireflySideHasContainers);
            return iterator;
        } else {
            iterator = Collections.emptyIterator();
            return iterator;
        }
    }

    private <R extends Element> Iterator<R> phatEdges(final FireflyGraph graph) {
        Iterator<R> iterator;
        final List<HasContainer> fireflySideHasContainers = FireflyBatchReadHelper
                .splitHasContainers(graph, returnClass, hasContainers).all;

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
                if (fireflyTestAll(e, this.hasContainers)) {
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

    static private <E extends Element> Iterator<E> hasContainerCheckedIterator(final Iterator<E> iterator, final List<HasContainer> minimalHasContainers) {
        return new HasContainerIterator<>(iterator, minimalHasContainers);
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

    @Override
    public String toString() {
        if (this.hasContainers.isEmpty()) {
            return StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.runType, Arrays.toString(this.ids));
        } else {
            return (null == this.ids || 0 == this.ids.length) ?
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.runType, this.hasContainers) :
                    StringFactory.stepString(this, this.returnClass.getSimpleName().toLowerCase(), this.runType, Arrays.toString(this.ids), this.hasContainers);
        }
    }
}
