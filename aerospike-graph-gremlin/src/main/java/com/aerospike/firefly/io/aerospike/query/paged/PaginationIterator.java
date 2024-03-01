package com.aerospike.firefly.io.aerospike.query.paged;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class PaginationIterator<E> implements CloseableIterator<E> {
    private boolean isClosed = false;
    private final Queue<E> queue = new ConcurrentLinkedQueue<>();
    private CountDownLatch latch = new CountDownLatch(1);
    private final Object lock = new Object();
    private final FireflyGraph graph;
    private final Runnable closeCallback;

    public PaginationIterator(final FireflyGraph graph, final Runnable closeCallback) {
        this.graph = graph;
        this.closeCallback = closeCallback;
    }

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
                boolean succeeded = latch.await(graph.getBaseGraph().PAGINATION_PAGE_MAX_WAIT, TimeUnit.MILLISECONDS);
                if (!succeeded) {
                    throw new RuntimeException("Timeout waiting for more records in PaginationIterator. " +
                            "State: " + isClosed + " " + queue.isEmpty() + ".");
                }
            } catch (final InterruptedException e) {
                // Unexpected interrupt, just go back to waiting.
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
            if (!isClosed) {
                isClosed = true;
            }
            latch.countDown();
        }
        closeCallback.run();
    }

    public void add(final E e) {
        synchronized (lock) {
            queue.add(e);
            latch.countDown();
        }
    }
}
