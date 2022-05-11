package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Iterator;
import java.util.function.Function;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public abstract class FireflyElementIterator<E> implements CloseableIterator<E> {
    private final AerospikeConnection db;
    private final Iterator<?> idIterator;
    private final Function<Object,E> fn;

    protected FireflyElementIterator(AerospikeConnection db, Iterator<?> idIterator, Function<Object, E> fn) {
        this.db = db;
        this.idIterator = idIterator;
        this.fn = fn;
    }

    @Override
    public boolean hasNext() {
        return this.idIterator.hasNext();
    }

    @Override
    public E next() {
        return fn.apply(this.idIterator.next());
    }
}
