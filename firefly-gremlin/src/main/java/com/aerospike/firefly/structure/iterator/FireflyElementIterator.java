package com.aerospike.firefly.structure.iterator;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;
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
    private final Iterator<FireflyId> idIterator;
    private final Function<FireflyId, E> fn;

    protected FireflyElementIterator(AerospikeConnection db, Iterator<FireflyId> idIterator, Function<FireflyId, E> fn) {
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
        final FireflyId nextId = this.idIterator.next();
        final E res = fn.apply(nextId);
        if (res == null)
            throw new NoSuchElementException(nextId.toString());
        return res;
    }
}
