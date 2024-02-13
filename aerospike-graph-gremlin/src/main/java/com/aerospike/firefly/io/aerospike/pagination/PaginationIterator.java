package com.aerospike.firefly.io.aerospike.pagination;

import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class PaginationIterator<E> implements CloseableIterator<E> {
    boolean isClosed = false;
    final Queue<E> queue = new ConcurrentLinkedQueue<>();
    CountDownLatch latch = new CountDownLatch(1);
    final Object lock = new Object();

    @Override
    public boolean hasNext() {
        while (queue.isEmpty()) {
            synchronized (lock) {
                latch = new CountDownLatch(1);
                if (isClosed && queue.isEmpty()) {
                    return false;
                } else if (!queue.isEmpty()) {
                    return true;
                }
            }
            try {
                boolean succeeded = latch.await(5, TimeUnit.SECONDS);
                if (!succeeded) {
                    throw new RuntimeException("Unexpected timeout in PaginationIterator.");
                }
            } catch (final InterruptedException e) {
                Thread.currentThread().interrupt();
                return false;
            }
        }
        return true;
    }

    @Override
    public E next() {
        if (hasNext()) {
            return queue.poll();
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            latch.countDown();
            if (!isClosed) {
                isClosed = true;
            }
        }
    }

    public void add(E e) {
        synchronized (lock) {
            latch.countDown();
            queue.add(e);
        }
    }
}
