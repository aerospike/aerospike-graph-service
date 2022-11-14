package com.aerospike.firefly.structure.iterator;

import com.aerospike.firefly.io.AerospikeConnection;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public abstract class FireflyElementIterator<E> implements CloseableIterator<E> {
    private final AerospikeConnection db;
    private final Iterator<?> idIterator;
    private final Function<Object, E> fn;
    private final Function<Object, Boolean> existsFn;

    protected FireflyElementIterator(AerospikeConnection db, Iterator<?> idIterator,Function<Object, Boolean> existsFn, Function<Object, E> fn) {
        this.db = db;
        this.idIterator = idIterator;
        this.fn = fn;
        this.existsFn = existsFn;

    }

    @Override
    public boolean hasNext() {
        return this.idIterator.hasNext();
    }

    @Override
    public E next() {
        final Object nextId = this.idIterator.next();
        final E res = fn.apply(nextId);
        if (res == null)
            throw new NoSuchElementException(nextId.toString());
        return res;
    }
}
