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

package com.aerospike.firefly.olap.process.traversal.step;

import com.aerospike.firefly.olap.codec.RowCodec;
import com.aerospike.firefly.util.ReflectionHelper;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.IdentityTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.ByModulating;
import org.apache.tinkerpop.gremlin.process.traversal.step.ComparatorHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.Seedable;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.javatuples.Pair;

import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public class SparkSortStep<S, C extends Comparable> extends CollectingBarrierStep<S>
        implements ComparatorHolder<S, C>, TraversalParent, ByModulating, Seedable, SparkOperation {
    private final OrderGlobalStep<S, C> step;

    public SparkSortStep(final Traversal.Admin traversal) {
        this(new OrderGlobalStep<>(traversal));
    }

    public SparkSortStep(final OrderGlobalStep<S, C> step) {
        super(step.getTraversal());
        this.step = step;
        this.step.setId(getId());

        // add default comparator. See explanation in OrderGlobalStep.getComparators()
        final List<Pair<Traversal.Admin<S, C>, Comparator<C>>> comparators = getComparators();
        if (comparators.size() == 1 && comparators.get(0).getValue0() instanceof IdentityTraversal && comparators.get(0).getValue1().equals(Order.asc)) {
            modulateBy(new IdentityTraversal());
        }
    }

    // proxy calls
    @Override
    public boolean hasNext() {
        return step.hasNext();
    }

    @Override
    public Traverser.Admin next() {
        final Traverser.Admin traverser = step.next();

        traverser.setStepId(this.getNextStep().getId());

        return traverser;
    }

    @Override
    public void addStarts(final Iterator<Traverser.Admin<S>> starts) {
        step.addStarts(starts);
    }

    @Override
    public void addStart(final Traverser.Admin<S> start) {
        step.addStart(start);
    }

    @Override
    public boolean hasNextBarrier() {
        return step.hasNextBarrier();
    }

    @Override
    public TraverserSet<S> nextBarrier() throws NoSuchElementException {
        final TraverserSet<S> barrier = step.nextBarrier();
        barrier.forEach(traverser -> traverser.setStepId(this.getNextStep().getId()));
        return barrier;
    }

    @Override
    public void addBarrier(final TraverserSet<S> barrier) {
        step.addBarrier(barrier);
        // OrderGlobalStep will try to sort already sorted data
        ReflectionHelper.setFieldValue(CollectingBarrierStep.class, step, "barrierConsumed", true);
    }

    @Override
    public void setId(final String id) {
        super.setId(id);
        step.setId(id);
    }

    @Override
    public void resetSeed(long seed) {
        step.resetSeed(seed);
    }

    @Override
    public void barrierConsumer(final TraverserSet<S> traverserSet) {
        step.barrierConsumer(traverserSet);
    }

    @Override
    public void processAllStarts() {
        step.processAllStarts();
    }

    public void setLimit(final long limit) {
        step.setLimit(limit);
    }

    public long getLimit() {
        return step.getLimit();
    }

    @Override
    public void addComparator(final Traversal.Admin<S, C> traversal, final Comparator<C> comparator) {
        step.addComparator(traversal, comparator);
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> traversal) {
        step.modulateBy(traversal);
    }

    @Override
    public void modulateBy(final Traversal.Admin<?, ?> traversal, final Comparator comparator) {
        step.modulateBy(traversal, comparator);
    }

    @Override
    public void replaceLocalChild(final Traversal.Admin<?, ?> oldTraversal, final Traversal.Admin<?, ?> newTraversal) {
        step.replaceLocalChild(oldTraversal, newTraversal);
    }

    @Override
    public List<Pair<Traversal.Admin<S, C>, Comparator<C>>> getComparators() {
        return step.getComparators();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, getLimit(), step.getComparators());
    }

    @Override
    public int hashCode() {
        return step.hashCode();
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return step.getRequirements();
    }

    @Override
    public List<Traversal.Admin<S, C>> getLocalChildren() {
        return step.getLocalChildren();
    }

    @Override
    public SparkSortStep<S, C> clone() {
        return new SparkSortStep(step.clone());
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        step.setTraversal(parentTraversal);
    }

    @Override
    public MemoryComputeKey getMemoryComputeKey() {
        // just add all because sorting done in spark
        return MemoryComputeKey.of(this.getId(), Operator.addAll, false, true);
    }

    // end of proxy calls

    @Override
    public Dataset<Row> operate(final Dataset<Row> df) {
        final List<Pair<Traversal.Admin<S, C>, Comparator<C>>> comparators = getComparators();
        final Column[] sortColumns = new Column[comparators.size()];
        for (int i = 0; i < comparators.size(); i++) {
            Column sortColumn;
            if (comparators.get(i).getValue1().equals(Order.asc)) {
                sortColumn = new Column(RowCodec.NATIVE_COL_PREFIX + i);
            } else if (comparators.get(i).getValue1().equals(Order.desc)) {
                sortColumn = new Column(RowCodec.NATIVE_COL_PREFIX + i).desc();
            } else {
                throw new IllegalArgumentException("Unsupported sort order: " + comparators.get(i).getValue1());
            }
            sortColumns[i] = sortColumn;
        }

        // spark limit() doesn't know about bulking
        return step.getLimit() == Long.MAX_VALUE || step.getLimit() < 0 ? df.orderBy(sortColumns) : df.orderBy(sortColumns).limit((int) step.getLimit());
    }

    @Override
    public int sparkColumnsCount() {
        return getComparators().size();
    }

    @Override
    public boolean canContinueDistributed() {
        // if last step
        if (getNextStep() instanceof EmptyStep) return true;

        // or there is another collecting barrier which will set own order
        Step next = getNextStep();
        while (!(next instanceof EmptyStep)) {
            if (next instanceof CollectingBarrierStep) {
                // special case for following sorting by Order.shuffle
                return !(next instanceof OrderGlobalStep)
                        || ((OrderGlobalStep) next).getComparators().stream().noneMatch(c -> ((Pair) c).getValue1() == Order.shuffle);
            }
            next = next.getNextStep();
        }

        return false;
    }
}
