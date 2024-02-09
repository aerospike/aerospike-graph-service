package com.aerospike.firefly.io.aerospike.pagination;

import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Consumer;

public abstract class PageFetcher<E> {
    private static final Logger LOG = LoggerFactory.getLogger(ScanPageFetcher.class);
    final FireflyGraph graph;
    final ExecutorService readLoopExecutorService;

    final BlockingQueue<Page> pageQueue;
    private final FireflyGraph.TransformKeyRecord<E> transformKeyRecord;
    final PartitionFilter filter;

    public PageFetcher(final FireflyGraph graph,
                       final int maxQueueSize,
                       final FireflyGraph.TransformKeyRecord<E> transformKeyRecord) {
        this.graph = graph;
        this.filter = PartitionFilter.all();
        this.readLoopExecutorService = Executors.newSingleThreadExecutor();
        this.pageQueue = new LinkedBlockingQueue<>(maxQueueSize);
        this.transformKeyRecord = transformKeyRecord;
    }

    protected abstract void readPage();

    public Iterator<E> startQuery() {
        // Start loop.
        readLoopExecutorService.submit(() -> {
            while (true) {
                if (readLoopExecutorService.isShutdown()) {
                    try {
                        pageQueue.put(new PoisonPill());
                    } catch (InterruptedException e) {
                        LOG.error("Error adding poison pill.", e);
                    }
                    return;
                }

                if (filter.isDone()) {
                    readLoopExecutorService.shutdown();
                    continue;
                }
                readPage();
            }
        });
        return new PageFetcher.PageIterator();
    }


    static class PoisonPill extends Page {
        public PoisonPill() {
            super(List.of());
        }
    }

    static class ErrorPage extends Page {
        final String errorMessage;

        public ErrorPage(final String errorMessage) {
            super(List.of());
            this.errorMessage = errorMessage;
        }
    }

    static class Page {
        public List<KeyRecord> keyRecords;

        public Page(final List<KeyRecord> keyRecords) {
            this.keyRecords = keyRecords;
        }

        public void close() {
            keyRecords.clear();
        }
    }

    public class PageIterator implements CloseableIterator<E> {
        final Queue<E> currentList;
        boolean isEmpty = false;
        boolean isClosed = false;

        PageIterator() {
            this.currentList = new LinkedList<>();
        }

        private void removePage() {
            // Check if possible.
            // No more data is coming in and there's no more pages available.
            if (readLoopExecutorService.isTerminated()) {
                if (pageQueue.isEmpty()) {
                    isEmpty = true;
                    return;
                }
            }

            try {
                final Page page = pageQueue.take();
                if (page instanceof PoisonPill) {
                    isEmpty = true;
                    return;
                } else if (page instanceof ErrorPage) {
                    throw new RuntimeException(((ErrorPage) page).errorMessage);
                }
                page.keyRecords.forEach((keyRecord) -> currentList.add(transformKeyRecord.transform(keyRecord)));
                page.close();
            } catch (final InterruptedException e) {
                LOG.error("Error removing page.", e);
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public boolean hasNext() {
            if (isClosed) {
                return false;
            }

            if (!currentList.isEmpty()) {
                return true;
            }

            while (currentList.isEmpty()) {
                removePage();
                if (isEmpty) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public E next() {
            if (!hasNext()) {
                throw new NoSuchElementException();
            } else {
                return currentList.remove();
            }
        }

        @Override
        public void close() {
            if (!isClosed) {
                isClosed = true;
                shutdown();
            }
        }
    }

    public void shutdown() {
        // Signal to readLoopExecutor that it needs to shut down.
        readLoopExecutorService.shutdown();
    }
}
