package com.aerospike.firefly.structure.iterator;

import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.io.Serializable;

public class FireflyCloseableDoubleIterator<T> implements CloseableIterator<T>, Serializable {

    private T a;
    private T b;
    private char current = 'a';

    protected FireflyCloseableDoubleIterator(final T a, final T b) {
        this.a = a;
        this.b = b;
    }

    @Override
    public boolean hasNext() {
        return this.current != 'x';
    }

    @Override
    public void remove() {
        if (this.current == 'b')
            this.a = null;
        else if (this.current == 'x')
            this.b = null;
    }

    @Override
    public T next() {
        if (this.current == 'x')
            throw FastNoSuchElementException.instance();
        else {
            if (this.current == 'a') {
                this.current = 'b';
                return this.a;
            } else {
                this.current = 'x';
                return this.b;
            }
        }
    }
}
