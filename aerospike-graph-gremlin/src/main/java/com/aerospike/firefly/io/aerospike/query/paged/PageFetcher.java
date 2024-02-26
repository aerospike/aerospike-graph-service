package com.aerospike.firefly.io.aerospike.query.paged;

import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIterator;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

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
                try {
                    if (readLoopExecutorService.isShutdown()) {
                        try {
                            pageQueue.put(new PoisonPill());
                        } catch (final InterruptedException e) {
                            LOG.error("Error adding poison pill.", e);
                            Thread.currentThread().interrupt();
                        }
                        return;
                    }

                    if (filter.isDone()) {
                        readLoopExecutorService.shutdown();
                        continue;
                    }
                    readPage();
                } catch (final Throwable e) {
                    signalError("Unexpected error while reading " + e.getMessage());
                }
            }
        });
        return new PageFetcher.PageIterator();
    }


    static class PoisonPill extends Page {

        public PoisonPill() {
            super(CloseableIterator.EmptyCloseableIterator.instance());
        }
    }

    static class ErrorPage extends Page {
        final String errorMessage;

        public ErrorPage(final String errorMessage) {
            super(CloseableIterator.EmptyCloseableIterator.instance());
            this.errorMessage = errorMessage;
        }
    }

    static class Page {
        public CloseableIterator<KeyRecord> keyRecords;

        public Page(final CloseableIterator<KeyRecord> keyRecords) {
            this.keyRecords = keyRecords;
        }
    }

    public class PageIterator implements CloseableIterator<E> {
        CloseableIterator<KeyRecord> currentIterator = FireflyCloseableIterator.EmptyCloseableIterator.instance();;
        boolean isEmpty = false;
        boolean isClosed = false;

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

                currentIterator = page.keyRecords;
            } catch (final InterruptedException e) {
                LOG.error("Error removing page.", e);
                Thread.currentThread().interrupt();
            }
        }

        @Override
        public boolean hasNext() {
            if (isEmpty) {
                return false;
            }

            if (isClosed) {
                return false;
            }

            while (!currentIterator.hasNext()) {
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
                return transformKeyRecord.transform(currentIterator.next());
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

    protected void signalError(final String error) {
        try {
            LOG.error(error);
            shutdown();
            pageQueue.put(new ErrorPage("Error reading query: " + error));
        } catch (final InterruptedException e2) {
            LOG.error("Error adding signalling error to iterator.", e2);
            Thread.currentThread().interrupt();
        }
    }
}
