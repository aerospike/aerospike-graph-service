package com.aerospike.firefly.structure.iterator;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Iterator;
import java.util.function.Function;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public abstract class FireflyElementIterator<E> implements CloseableIterator<E> {
    private final AerospikeConnection db;
    private final Iterator<FireflyId> idIterator;
    private final Function<FireflyId, E> fn;
    private boolean hasNext = false;
    private E buffer = null;

    protected FireflyElementIterator(AerospikeConnection db, Iterator<FireflyId> idIterator, Function<FireflyId, E> fn) {
        this.db = db;
        this.idIterator = idIterator;
        this.fn = fn;
        bufferNext();
    }

    @Override
    public boolean hasNext() {
        return this.hasNext;
    }

    @Override
    public E next() {
        if (this.hasNext) {
            final E element = this.buffer;
            this.buffer = null;
            bufferNext();
            return element;
        } else {
            throw FastNoSuchElementException.instance();
        }
    }

    /**
     * This buffers the next element in memory in order to ensure correctness of the iterator since a FireflyId may map
     * to a non-existent record and thus returns null instead of skipping over that element. This method filters out
     * the non-existent records.
     */
    private void bufferNext() {
        while (this.idIterator.hasNext() && this.buffer == null) {
            this.buffer = this.fn.apply(this.idIterator.next());
        }

        if (this.buffer != null) {
            this.hasNext = true;
        } else {
            // IDs have been exhausted and could not find any more valid elements.
            this.hasNext = false;
        }
    }
}
