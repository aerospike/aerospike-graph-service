package com.aerospike.firefly.structure.iterator;

import com.aerospike.client.query.RecordSet;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Iterator;

public class FireflyCloseableIterator<E> implements CloseableIterator<E> {

    final RecordSet rs;
    final Iterator<E> i;

    public FireflyCloseableIterator(final Iterator<E> i) {
        this.rs = null;
        this.i = i;
    }

    public FireflyCloseableIterator(final RecordSet rs) {
        this.rs = rs;
        this.i = (Iterator<E>) rs.iterator();
    }

    @Override
    public void close() {
        if (rs != null) {
            rs.close();
        }
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
