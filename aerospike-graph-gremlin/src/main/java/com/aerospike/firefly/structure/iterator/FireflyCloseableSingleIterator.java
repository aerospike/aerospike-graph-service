package com.aerospike.firefly.structure.iterator;

import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.io.Serializable;

public class FireflyCloseableSingleIterator<T> implements CloseableIterator<T>, Serializable {
    private T t;
    private boolean alive = true;

    protected FireflyCloseableSingleIterator(final T t) {
        this.t = t;
    }

    @Override
    public boolean hasNext() {
        return this.alive;
    }

    @Override
    public void remove() {
        this.t = null;
        this.alive = false;
    }

    @Override
    public T next() {
        if (!this.alive)
            throw FastNoSuchElementException.instance();
        else {
            this.alive = false;
            return t;
        }
    }

    @Override
    public void close() {
        // No iterator to close underneath here.
        t = null;
        this.alive = false;
    }
}
