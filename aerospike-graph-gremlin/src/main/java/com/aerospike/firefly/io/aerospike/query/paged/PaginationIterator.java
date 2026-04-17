/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.io.aerospike.query.paged;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class PaginationIterator<E> implements CloseableIterator<E> {
    private static final Logger LOGGER = LoggerFactory.getLogger(PaginationIterator.class);
    private boolean isClosed = false;
    private final Queue<E> queue = new ConcurrentLinkedQueue<>();
    private CountDownLatch latch = new CountDownLatch(1);
    private final Object lock = new Object();
    private final FireflyGraph graph;
    private final Runnable closeCallback;
    private final long timeout;

    public PaginationIterator(final FireflyGraph graph, final Runnable closeCallback, final long timeout) {
        this.graph = graph;
        this.closeCallback = closeCallback;
        this.timeout = timeout;
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
                boolean succeeded = latch.await(Math.min(timeout, graph.getBaseGraph().getConfig().paginationPageMaxWait), TimeUnit.MILLISECONDS);
                if (!succeeded) {
                    throw new RuntimeException("Timeout waiting for more records in PaginationIterator. " +
                            "State: " + isClosed + " " + queue.isEmpty() + ".");
                }
            } catch (final InterruptedException e) {
                throw new TraversalInterruptedException();
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
                closeCallback.run();
            }
            latch.countDown();
        }
    }

    public void add(final E e) {
        synchronized (lock) {
            queue.add(e);
            latch.countDown();
        }
    }
}
