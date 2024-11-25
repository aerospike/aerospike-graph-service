package com.aerospike.firefly.structure.iterator;

import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Iterator;

public class FireflyCloseableIterator<E> implements CloseableIterator<E> {
    final Iterator<E> i;

    public FireflyCloseableIterator(final Iterator<E> i) {
        this.i = i;
    }

    @Override
    public void close() {
        CloseableIterator.closeIterator(i);
    }

    @Override
    public boolean hasNext() {
        return i.hasNext();
    }

    @Override
    public E next() {
        return i.next();
    }
}
