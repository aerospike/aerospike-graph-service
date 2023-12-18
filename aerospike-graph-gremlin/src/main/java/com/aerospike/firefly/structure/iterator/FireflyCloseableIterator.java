package com.aerospike.firefly.structure.iterator;

import com.aerospike.client.query.RecordSet;
import com.aerospike.firefly.io.aerospike.ConcurrentScanRecordSequenceListener;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.Iterator;

public class FireflyCloseableIterator<E> implements CloseableIterator<E> {

    final RecordSet rs;
    final ConcurrentScanRecordSequenceListener listener;
    final Iterator<E> i;

    public FireflyCloseableIterator(final Iterator<E> i) {
        this.rs = null;
        this.i = i;
        this.listener = null;
    }

    public FireflyCloseableIterator(final RecordSet rs) {
        this.rs = rs;
        this.i = (Iterator<E>) rs.iterator();
        this.listener = null;
    }

    public FireflyCloseableIterator(final ConcurrentScanRecordSequenceListener listener) {
        this.rs = null;
        this.i = (Iterator<E>) listener.iterator();
        this.listener = listener;
    }

    @Override
    public void close() {
        if (listener != null) {
            listener.terminateScan();
        }
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
