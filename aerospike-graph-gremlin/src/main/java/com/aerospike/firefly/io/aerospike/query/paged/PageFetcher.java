package com.aerospike.firefly.io.aerospike.query.paged;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIterator;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalInterruptedException;
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
    private static final Logger LOG = LoggerFactory.getLogger(PageFetcher.class);
    protected final FireflyGraph graph;
    private final ExecutorService readLoopExecutorService;
    protected final BlockingQueue<Page> pageQueue;
    private final FireflyGraph.TransformKeyRecord<E> transformKeyRecord;
    protected final PartitionFilter filter;
    protected final String indexName;

    public PageFetcher(final FireflyGraph graph,
                       final int maxQueueSize,
                       final FireflyGraph.TransformKeyRecord<E> transformKeyRecord) {
        this(graph, maxQueueSize, transformKeyRecord, null);
    }

    public PageFetcher(final FireflyGraph graph,
                       final int maxQueueSize,
                       final FireflyGraph.TransformKeyRecord<E> transformKeyRecord,
                       final String indexName) {
        this.graph = graph;
        this.filter = PartitionFilter.all();
        this.readLoopExecutorService = Executors.newSingleThreadExecutor();
        this.pageQueue = new LinkedBlockingQueue<>(maxQueueSize);
        this.transformKeyRecord = transformKeyRecord;
        this.indexName = indexName;
    }

    protected boolean isDone() {
        return filter.isDone();
    }

    protected abstract void readPage();

    public Iterator<E> startQuery() {
        // Start loop.
        readPages();
        return new PageFetcher.PageIterator();
    }

    public BlockingQueue<Page> startQueryPagesDirect() {
        // Start loop.
        readPages();
        return this.pageQueue;
    }

    private void readPages() {
        readLoopExecutorService.submit(() -> {
            while (true) {
                try {
                    if (readLoopExecutorService.isShutdown()) {
                        try {
                            pageQueue.put(new PoisonPill());
                        } catch (final InterruptedException e) {
                            LOG.error("Error adding poison pill.", e);
                            throw new TraversalInterruptedException();
                        }
                        return;
                    }
                    if (isDone()) {
                        readLoopExecutorService.shutdown();
                        continue;
                    }
                    readPage();
                } catch (final Throwable e) {
                    signalError("Unexpected error while reading " + e.getMessage(), e);
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
            return this.exception instanceof AerospikeException
                    && ((AerospikeException) this.exception).getResultCode() == ResultCode.INDEX_NOTFOUND;
        }
    }

    public static class Page {
        public CloseableIterator<KeyRecord> keyRecords;

        public Page(final CloseableIterator<KeyRecord> keyRecords) {
            this.keyRecords = keyRecords;
        }
    }

    public class PageIterator implements CloseableIterator<E> {
        private static final String NO_ERROR = "";
        private static final String INDEX_DROPPED = "INDEX_DROPPED";
        private CloseableIterator<KeyRecord> currentIterator = FireflyCloseableIterator.EmptyCloseableIterator.instance();
        ;
        private boolean isEmpty = false;
        private boolean isClosed = false;
        private String errorMessage = NO_ERROR;
        private Throwable error = null;

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
                    final StringBuilder sb = new StringBuilder();
                    sb.append("This query is temporarily unavailable due to the index it utilizes");
                    if (indexName != null) {
                        sb.append(", \"").append(indexName).append("\",");
                    }
                    sb.append(" being recently dropped. Please wait ");
                    sb.append(graph.getBaseGraph().INDEX_METADATA_UPDATE_FREQUENCY / 1000);
                    sb.append(" seconds and try again.");
                    throw new RuntimeException(sb.toString());
                }
                if (!NO_ERROR.equals(errorMessage)) {
                    throw new RuntimeException(errorMessage, error);
                }
                return transformKeyRecord.transform(currentIterator.next());
            }
        }

        @Override
        public void close() {
            if (!isClosed) {
                isClosed = true;
                shutdown();

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
        if (graph.getBaseGraph().PAGINATION_SHUTDOWN_WAIT != 0) {
            try {
                if (!readLoopExecutorService.awaitTermination(graph.getBaseGraph().PAGINATION_SHUTDOWN_WAIT,
                        java.util.concurrent.TimeUnit.MILLISECONDS)) {
                    readLoopExecutorService.shutdownNow();
                }
            } catch (final InterruptedException e) {
            }
        }
    }

    protected void signalError(final String error) {
        signalError(error, null);
    }

    protected void signalError(final String error, final Throwable exception) {
        LOG.error("{} attempting to signal error to iterator.", error);
        shutdown();
        boolean success = false;
        for (int attemptCount = 0; attemptCount < 3; attemptCount++) {
            pageQueue.clear();
            if (!pageQueue.offer(new ErrorPage(error, exception))) {
                success = true;
                break;
            } else {
                LOG.warn("Failed to send error signal to iterator. Attempting to send again.");
            }
        }
        if (!success) {
            LOG.error("Failed to send error signal to iterator.");
        }
    }
}
