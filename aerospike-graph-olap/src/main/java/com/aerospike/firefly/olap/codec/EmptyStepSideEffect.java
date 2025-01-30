package com.aerospike.firefly.olap.codec;

import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.util.Collections;
import java.util.Iterator;
import java.util.Set;

public class EmptyStepSideEffect <S, E> implements Step<S, E>, TraversalParent {
    final Traversal.Admin<?, ?> traversal;

    public static <S, E> EmptyStepSideEffect<S, E> instance(final Traversal<?, ?> traversal) {
        return new EmptyStepSideEffect<>(traversal);
    }

    private EmptyStepSideEffect(final Traversal<?, ?> traversal) {
        this.traversal = (Traversal.Admin<?, ?>) traversal;
    }

    @Override
    public void addStarts(final Iterator<Traverser.Admin<S>> starts) { }

    @Override
    public boolean hasStarts() {
        return false;
    }

    @Override
    public void addStart(final Traverser.Admin<S> start) { }

    @Override
    public void setPreviousStep(final Step<?, S> step) { }

    @Override
    public void reset() { }

    @Override
    public Step<?, S> getPreviousStep() {
        return EmptyStep.instance();
    }

    @Override
    public void setNextStep(final Step<E, ?> step) { }

    @Override
    public Step<E, ?> getNextStep() {
        return EmptyStep.instance();
    }

    @Override
    public <A, B> Traversal.Admin<A, B> getTraversal() {
        return (Traversal.Admin<A, B>) traversal.asAdmin();
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> traversal) { }

    @Override
    @SuppressWarnings("CloneDoesntCallSuperClone")
    public EmptyStepSideEffect<S, E> clone() {
        return this;
    }

    @Override
    public Set<String> getLabels() {
        return Collections.emptySet();
    }

    @Override
    public void addLabel(final String label) { }

    @Override
    public void removeLabel(final String label) { }

    @Override
    public void clearLabels() { }

    @Override
    public void setId(final String id) { }

    @Override
    public String getId() {
        return Traverser.Admin.HALT;
    }

    @Override
    public boolean hasNext() {
        return false;
    }

    @Override
    public Traverser.Admin<E> next() {
        throw FastNoSuchElementException.instance();
    }

    @Override
    public int hashCode() {
        return -1691648095;
    }

    @Override
    public boolean equals(final Object object) {
        return object instanceof EmptyStep;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.emptySet();
    }
}