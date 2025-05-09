package com.aerospike.firefly.io.aerospike.query.paged;

import com.aerospike.client.ResultCode;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIterator;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.aerospike.firefly.util.exceptions.DataModelVersionMismatchException;
import com.aerospike.firefly.util.exceptions.GraphError;
import com.aerospike.firefly.util.exceptions.SindexRecentlyDroppedException;
import com.aerospike.firefly.util.exceptions.ThreadLimitExceededException;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class PageFetcher<E> {
    private static final Logger LOG = LoggerFactory.getLogger(PageFetcher.class);
    protected final FireflyGraph graph;
    protected final ExecutorService readLoopExecutorService;
    protected final BlockingQueue<Page> pageQueue;
    private final FireflyGraph.TransformKeyRecord<E> transformKeyRecord;
    protected final PartitionFilter filter;
    protected final String indexName;
    protected AtomicBoolean isClosing = new AtomicBoolean(false);

    public PageFetcher(final FireflyGraph graph,
                       final FireflyGraph.TransformKeyRecord<E> transformKeyRecord,
                       final String indexName) {
        this(
                graph,
                transformKeyRecord,
                indexName,
                PartitionFilter.all(),
                Executors.newSingleThreadExecutor(r -> {
                    final Thread t = new Thread(r);
                    t.setName("Aerospike-Graph-Pagination-Worker-" + t.getId());
                    t.setDaemon(true);
                    return t;
                }),
                new LinkedBlockingQueue<>(graph.getBaseGraph().PAGINATION_PAGE_QUEUE_SIZE)
        );
    }

    public PageFetcher(final FireflyGraph graph,
                       final FireflyGraph.TransformKeyRecord<E> transformKeyRecord,
                       final String indexName,
                       final PartitionFilter partitionFilter,
                       final ExecutorService readLoopExecutorService,
                       final BlockingQueue<Page> pageQueue) {
        this.graph = graph;
        this.filter = partitionFilter;
        this.readLoopExecutorService = readLoopExecutorService;
        this.pageQueue = pageQueue;
        this.transformKeyRecord = transformKeyRecord;
        this.indexName = indexName;
    }

    protected boolean isDone() {
        return filter.isDone();
    }

    protected abstract void readPage() throws InterruptedException;

    public Iterator<E> startQuery() {
        // Start loop.
        readPages();
        return new PageFetcher.PageIterator();
    }

    public BlockingQueue<Page> startQueryDirect() {
        // Start loop.
        readPages();
        return pageQueue;
    }

    protected void readPages() {
        readLoopExecutorService.submit(() -> {
            while (true) {
                try {
                    if (readLoopExecutorService.isShutdown()) {
                        try {
                            pageQueue.put(new PoisonPill());
                        } catch (final InterruptedException e) {
                            signalError("Interrupted while attempting to add poison pill.", e);
                        }
                        return;
                    }
                    if (isDone()) {
                        readLoopExecutorService.shutdown();
                        continue;
                    }
                    readPage();
                } catch (final Throwable e) {
                    if (e.getMessage() == null) {
                        signalError("Unexpected error while reading.", e);
                    } else {
                        signalError("Unexpected error while reading " + e.getMessage(), e);
                    }
                    return;
                }
            }
        });
    }

    public static class PoisonPill extends Page {

        public PoisonPill() {
            super(CloseableIterator.EmptyCloseableIterator.instance());
        }
    }

    public static class ErrorPage extends Page {
        public final String errorMessage;
        public final Throwable exception;

        public ErrorPage(final String errorMessage, final Throwable exception) {
            super(CloseableIterator.EmptyCloseableIterator.instance());
            this.errorMessage = errorMessage;
            this.exception = exception;
        }

        private boolean isIndexDropError() {
            return this.exception instanceof AerospikeGraphException
                    && ((AerospikeGraphException) this.exception).errorCode == ResultCode.INDEX_NOTFOUND;
        }
    }

    public static class Page {
        public CloseableIterator<KeyRecord> keyRecords;

        public Page(final CloseableIterator<KeyRecord> keyRecords) {
            this.keyRecords = keyRecords;
        }
    }


    public static class VertexPage extends Page {
        public CloseableIterator<FireflyVertex> vertices;

        public VertexPage(final CloseableIterator<FireflyVertex> vertices) {
            super(CloseableIterator.EmptyCloseableIterator.instance());
            this.vertices = vertices;
        }
    }

    public class PageIterator implements CloseableIterator<E> {
        private static final String NO_ERROR = "";
        private static final String INDEX_DROPPED = "INDEX_DROPPED";
        private CloseableIterator<KeyRecord> currentIterator = FireflyCloseableIterator.EmptyCloseableIterator.instance();
        private boolean isEmpty = false;
        private boolean isClosed = false;
        private String errorMessage = NO_ERROR;
        private Throwable error = null;

        private void removePage() {
            // Race condition protection - if an error has occurred already reading is stopped.
            if (!NO_ERROR.equals(errorMessage)) {
                return;
            }

            // Check if possible.
            // No more data is coming in and there's no more pages available.
            if (readLoopExecutorService.isTerminated()) {
                if (pageQueue.isEmpty()) {
                    isEmpty = true;
                    return;
                }
            }

            try {
                if (Thread.currentThread().isInterrupted()) {
                    close();
                    throw new TraversalInterruptedException();
                }
                final Page page = pageQueue.take();
                if (page instanceof PoisonPill) {
                    isEmpty = true;
                    return;
                } else if (page instanceof ErrorPage) {
                    final ErrorPage errorPage = (ErrorPage) page;
                    errorMessage = errorPage.isIndexDropError() ? INDEX_DROPPED : errorPage.errorMessage;
                    error = errorPage.exception;
                    if (error instanceof TraversalInterruptedException) {
                        throw new TraversalInterruptedException();
                    }
                    return;
                }

                currentIterator = page.keyRecords;
            } catch (final InterruptedException e) {
                close();
                throw new TraversalInterruptedException();
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
                if (!NO_ERROR.equals(errorMessage)) {
                    return true;
                }
                if (isEmpty) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public E next() {
            if (Thread.currentThread().isInterrupted()) {
                close();
                throw new TraversalInterruptedException();
            }
            if (!hasNext()) {
                throw new NoSuchElementException();
            } else {
                if (INDEX_DROPPED.equals(errorMessage)) {
                    try {
                        graph.fireflyIndexMetadata.updateMetadata();
                    } catch (final Exception e) {
                        LOG.warn("Updating Index metadata forcibly due to using a dropped index failed.", e);
                    }
                    final String displayName;
                    if (indexName != null) {
                        displayName = ", '" + indexName + "',";
                    } else {
                        displayName = "";
                    }
                    throw new SindexRecentlyDroppedException(displayName, graph.getBaseGraph().INDEX_METADATA_UPDATE_FREQUENCY / 1000);
                }
                if (!NO_ERROR.equals(errorMessage)) {
                    if (error instanceof AerospikeGraphException) {
                        throw (AerospikeGraphException) error;
                    } else {
                        throw new ThreadLimitExceededException();
                        //throw new RuntimeException(, error);
                    }
                }
                return transformKeyRecord.transform(currentIterator.next());
            }
        }

        @Override
        public void close() {
            isClosing.set(true);
            if (!isClosed) {
                isClosed = true;
                pageQueue.forEach(page -> {
                    if (!(page instanceof ErrorPage || page instanceof PoisonPill)) {
                        if (page.keyRecords != null) {
                            page.keyRecords.close();
                        }
                    }
                });
                shutdown();
                if (graph.getBaseGraph().PAGINATION_SHUTDOWN_WAIT != 0) {
                    try {
                        if (!readLoopExecutorService.awaitTermination(graph.getBaseGraph().PAGINATION_SHUTDOWN_WAIT,
                                java.util.concurrent.TimeUnit.MILLISECONDS)) {
                            readLoopExecutorService.shutdownNow();
                        }
                    } catch (final InterruptedException e) {
                    }
                }

                // Clean up any remaining pages.
                if (currentIterator != null) {
                    currentIterator.close();
                }

                // Technically there's a race condition that we signal for read loop to shutdown above,
                // the read loop hasn't gotten this yet and goes to start a new page, we dump our queue here
                // and the read loop adds a new page after we exit.
                // The effect of this is a single page in the queue that will never be read and will garbage collect
                // later and a single page of reading happening in the background.
                // The logic to fix that is quite a bit extra so this is an okay compromise to keep things simpler.
                while (!pageQueue.isEmpty()) {
                    try {
                        final Page page = pageQueue.remove();
                        if (!(page instanceof ErrorPage || page instanceof PoisonPill)) {
                            if (page.keyRecords != null) {
                                page.keyRecords.close();
                            }
                        }
                    } catch (final NoSuchElementException e) {
                        // Should never happen since we check isEmpty() first. Don't want to propagate this exception.
                        // Log warning in case it does happen we can investigate.
                        LOG.warn("Successfully recovered from NoSuchElementException while closing page iterator.", e);
                    }
                }
            }
        }
    }

    public void shutdown() {
        // Signal to readLoopExecutor that it needs to shut down.
        readLoopExecutorService.shutdown();
    }

    public void shutdownAwait() {
        // Signal to readLoopExecutor that it needs to shut down.
        readLoopExecutorService.shutdown();
        try {
            if (!readLoopExecutorService.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                readLoopExecutorService.shutdownNow();
            }
        } catch (final InterruptedException e) {
        }
    }

    protected void signalError(final String error) {
        signalError(error, null);
    }

    protected void signalError(final String error, final Throwable exception) {
        if (!isClosing.get()) {
            LOG.error("Attempting to signal error to iterator: {}", error);
        }
        try {
            if (!isClosing.get()) {
                shutdown();
            }
            boolean success = false;
            for (int attemptCount = 0; attemptCount < 3; attemptCount++) {
                pageQueue.clear();
                if (pageQueue.offer(new ErrorPage(error, exception))) {
                    success = true;
                    break;
                }
            }
            if (!success && !isClosing.get()) {
                LOG.error("Failed to send error signal to iterator.");
            }
        } catch (final Exception e) {
            if (!isClosing.get()) {
                LOG.error("Failed to signal error to iterator. Please contact support.", e);
            }
        }
    }
}
