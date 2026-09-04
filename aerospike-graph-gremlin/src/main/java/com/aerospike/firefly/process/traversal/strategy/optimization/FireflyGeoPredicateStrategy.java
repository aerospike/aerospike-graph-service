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

package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.predicate.GeoPredicate;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.FireflyGeoValue;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.ConnectiveP;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class FireflyGeoPredicateStrategy extends FireflyStrategyBase {

    public FireflyGeoPredicateStrategy() {
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        final Graph graph = traversal.asAdmin().getGraph().orElse(null);
        if (!(graph instanceof FireflyGraph)) {
            return;
        }
        final FireflyGraph fireflyGraph = (FireflyGraph) graph;
        for (final HasStep hasStep : TraversalHelper.getStepsOfClass(HasStep.class, traversal)) {
            final List<HasContainer> rewritten = new ArrayList<>();
            boolean changed = false;
            for (final HasContainer hasContainer : ((HasContainerHolder) hasStep).getHasContainers()) {
                final HasContainer rewrittenContainer = rewriteHasContainer(fireflyGraph, hasContainer);
                rewritten.add(rewrittenContainer);
                changed = changed || rewrittenContainer != hasContainer;
            }
            if (changed) {
                final List<HasContainer> toRemove = new ArrayList<>(hasStep.getHasContainers());
                for (final HasContainer container : toRemove) {
                    hasStep.removeHasContainer(container);
                }
                for (final HasContainer container : rewritten) {
                    hasStep.addHasContainer(container);
                }
            }
        }
    }

    private HasContainer rewriteHasContainer(final FireflyGraph graph, final HasContainer hasContainer) {
        if (hasContainer == null || hasContainer.getKey() == null || hasContainer.getValue() == null) {
            return hasContainer;
        }
        if (!graph.getBaseGraph().schemaManager.isRegisteredGeoProperty(hasContainer.getKey())) {
            if (hasContainer.getBiPredicate().equals(Compare.eq)
                    && FireflyGeoValue.isGeoCoordinateInput(hasContainer.getValue())) {
                return new HasContainer(hasContainer.getKey(), GeoPredicate.exactPoint((List<?>) hasContainer.getValue()));
            }
            return hasContainer;
        }
        if (hasContainer.getBiPredicate().equals(org.apache.tinkerpop.gremlin.process.traversal.Contains.within)) {
            final P<?> geoPredicate = rewriteWithinPredicate(hasContainer.getValue());
            if (geoPredicate != null) {
                return new HasContainer(hasContainer.getKey(), geoPredicate);
            }
        } else if (hasContainer.getBiPredicate().equals(Compare.eq)
                && FireflyGeoValue.isGeoCoordinateInput(hasContainer.getValue())) {
            return new HasContainer(hasContainer.getKey(), GeoPredicate.exactPoint((List<?>) hasContainer.getValue()));
        }
        return hasContainer;
    }

    private P<?> rewriteWithinPredicate(final Object value) {
        if (value instanceof ConnectiveP) {
            final ConnectiveP<?> connectiveP = (ConnectiveP<?>) value;
            final List<P<?>> predicates = new ArrayList<>();
            for (final Object predicate : connectiveP.getPredicates()) {
                if (predicate instanceof P) {
                    final P<?> rewritten = rewriteWithinPredicate(((P<?>) predicate).getValue());
                    if (rewritten != null) {
                        predicates.add(rewritten);
                    }
                }
            }
            return predicates.isEmpty() ? null : predicates.get(0);
        }
        if (!(value instanceof Collection)) {
            return null;
        }
        final Collection<?> values = (Collection<?>) value;
        if (values.size() == 3 && values.stream().allMatch(Number.class::isInstance)) {
            final Object[] array = values.toArray();
            return GeoPredicate.withinRadius(((Number) array[0]).doubleValue(),
                    ((Number) array[1]).doubleValue(),
                    ((Number) array[2]).doubleValue());
        }
        if (values.size() >= 4 && values.stream().allMatch(v -> v instanceof List && FireflyGeoValue.isCoordinatePair((List<?>) v))) {
            final List<List<Double>> ring = new ArrayList<>();
            for (final Object element : values) {
                final List<?> pair = (List<?>) element;
                ring.add(List.of(((Number) pair.get(0)).doubleValue(), ((Number) pair.get(1)).doubleValue()));
            }
            return GeoPredicate.withinRegion(ring);
        }
        return null;
    }
}
