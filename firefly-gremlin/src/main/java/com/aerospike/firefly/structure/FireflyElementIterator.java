package com.aerospike.firefly.structure;

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
    private final Function<Object,E> fn;

    protected FireflyElementIterator(AerospikeConnection db, Iterator<?> idIterator, Function<Object, E> fn) {
        this.db = db;
        this.idIterator = idIterator;
        this.fn = fn;
    }

    @Override
    public boolean hasNext() {
        //@todo performance
        //vertex ids should be in their own counter
        return this.idIterator.hasNext();
    }

    @Override
    public E next() {
        //@todo performance
        // we will not want to iterate thru removed ids
        // if it can be avoided
        E ele = null;
        while (ele == null && this.hasNext())
            ele = fn.apply(this.idIterator.next());
        if (ele == null)
            throw new NoSuchElementException();
        return ele;
    }
}
