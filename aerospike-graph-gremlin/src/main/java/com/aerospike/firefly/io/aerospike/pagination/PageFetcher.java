package com.aerospike.firefly.io.aerospike.pagination;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.client.query.RecordSet;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public abstract class PageFetcher<E> {
    private static final Logger LOG = LoggerFactory.getLogger(ScanPageFetcher.class);

    final FireflyGraph graph;
    final ExecutorService pageReaderExecutorService;
    final ExecutorService readLoopExecutorService;

    final BlockingQueue<Page> pageQueue;
    final Object LOCK1 = new Object();
    final Object LOCK2 = new Object();
    final Object PAGE_LOCK = new Object();
    CountDownLatch latch1;
    CountDownLatch latch2;
    final FireflyGraph.TransformKeyRecord<E> transformKeyRecord;
    final PartitionFilter filter;
    final int readThreadCount;

    public PageFetcher(final FireflyGraph graph,
                       final int readThreadCount,
                       final int maxQueueSize,
                       final FireflyGraph.TransformKeyRecord<E> transformKeyRecord) {
        this.graph = graph;
        this.filter = PartitionFilter.all();
        this.pageReaderExecutorService = Executors.newFixedThreadPool(readThreadCount);
        this.readLoopExecutorService = Executors.newSingleThreadExecutor();
        this.pageQueue = new LinkedBlockingQueue<>(maxQueueSize);
        this.readThreadCount = readThreadCount;
        this.latch1 = new CountDownLatch(1);
        this.latch2 = new CountDownLatch(1);
        this.transformKeyRecord = transformKeyRecord;
    }

    protected abstract void readPage(final AerospikeClient client, final Object PAGE_LOCK) throws InterruptedException;

    private void countdownLatch() {
        latch1.countDown();
    }

    public Iterator<E> startQuery() {
        final AerospikeClient client = graph.getBaseGraph().getClient();

        // Start the loop with a broken latch.
        latch1.countDown();

        // Start loop.
        final AtomicInteger pagesBeingRead = new AtomicInteger(0);
        readLoopExecutorService.submit(() -> {
            while (true) {
                if (readLoopExecutorService.isShutdown()) {
                    System.out.println("ReadLoop isShutdown");
                    pageReaderExecutorService.shutdown();
                    try {
                        pageReaderExecutorService.awaitTermination(1, java.util.concurrent.TimeUnit.SECONDS);
                    } catch (final InterruptedException e) {
                        LOG.error("Error shutting down page reader executor.", e);
                    }
                    pageQueue.add(new PoisonPill());
                    return;
                }
                if (filter.isDone()) {
                    System.out.println("Filter isDone");
                    readLoopExecutorService.shutdown();
                    pageReaderExecutorService.shutdown();
                    try {
                        pageReaderExecutorService.awaitTermination(1, java.util.concurrent.TimeUnit.SECONDS);
                    } catch (final InterruptedException e) {
                        LOG.error("Error shutting down page reader executor.", e);
                    }
                    pageQueue.add(new PoisonPill());
                    return;
                }

                // Wait for next event.
                synchronized (LOCK1) {
                    if (pageQueue.remainingCapacity() == 0) {
                        try {
                            System.out.println("Waiting for room in queue");
                            latch1.await();
                        } catch (final InterruptedException e) {
                            LOG.error("Error waiting for next event.", e);
                        }
                    }
                }
                synchronized (LOCK2) {
                    if (pagesBeingRead.get() > 2 * readThreadCount) {
                        try {
                            System.out.println("Waiting for read thread");
                            latch2.await();
                        } catch (final InterruptedException e) {
                            LOG.error("Error waiting for next event.", e);
                        }
                    }
                }

                final int emptyPageCount;

                // Read capacity, create latch. This way the latch can be awaited after queries have been submitted.
                synchronized (LOCK1) {
                    emptyPageCount = pageQueue.remainingCapacity() - pagesBeingRead.get();
                    latch1 = new CountDownLatch(1);
                    latch2 = new CountDownLatch(1);
                }
                System.out.println("Empty page count: " + emptyPageCount);
                for (int i = 0; i < emptyPageCount; i++) {
                    pagesBeingRead.incrementAndGet();
                    pageReaderExecutorService.submit(new ReadPage(client, pagesBeingRead, latch2, PAGE_LOCK));
                }
            }
        });
        return new PageFetcher.PageIterator();
    }

    class ReadPage implements Runnable {
        final AerospikeClient client;
        final CountDownLatch latch;
        final AtomicInteger pagesBeingRead;
        final Object PAGE_LOCK;

        public ReadPage(final AerospikeClient client, final AtomicInteger pagesBeingRead, final CountDownLatch latch2, final Object PAGE_LOCK) {
            this.client = client;
            this.latch = latch2;
            this.PAGE_LOCK = PAGE_LOCK;
            this.pagesBeingRead = pagesBeingRead;
        }

        @Override
        public void run() {
            // If we have queue'd tasks and were shut down, exit quickly.
            if (pageReaderExecutorService.isShutdown()) {
                System.out.println("page read shutdown");
                pagesBeingRead.decrementAndGet();
                latch2.countDown();
                return;
            }
            try {
                System.out.println("Reading page");
                readPage(client, PAGE_LOCK);
            } catch (final InterruptedException e) {
                LOG.error("Error reading query partition.", e);
            }
            pagesBeingRead.decrementAndGet();
            latch2.countDown();
        }
    }

    static class PoisonPill extends Page {
        public PoisonPill() {
            super((RecordSet) null);
        }
    }

    static class Page {
        final RecordSet recordSet;
        public List<KeyRecord> keyRecords;

        public Page(final RecordSet recordSet) {
            this.recordSet = recordSet;
            System.out.println("Got page with records");
            keyRecords = null;
        }

        public Page(final List<KeyRecord> keyRecords) {
            this.keyRecords = keyRecords;
            System.out.println("Got page with " + keyRecords.size() + " records");
            recordSet = null;
        }

        public void forEach(final Consumer<KeyRecord> consumer) {
            if (recordSet != null) {
                recordSet.forEach(consumer);
            } else {
                keyRecords.forEach(consumer::accept);
            }
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
                System.out.println("Taking page");
                final Page page = pageQueue.take();
                if (page instanceof PoisonPill) {
                    System.out.println("Poison pill");
                    isEmpty = true;
                    return;
                }

                countdownLatch();
                page.forEach((keyRecord) -> currentList.add(transformKeyRecord.transform(keyRecord)));
            } catch (InterruptedException e) {
                LOG.error("Error removing page.", e);
            }
        }

        @Override
        public boolean hasNext() {
            if (isClosed) {
                throw new IllegalStateException("Iterator is closed.");
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
            isClosed = true;
            System.out.println("close");
            shutdown();
        }
    }

    public void shutdown() {
        System.out.println("Shutdown");
        // Signal to readLoopExecutor that it needs to shut down.
        readLoopExecutorService.shutdown();

        // Countdown latch in case the read loop is blocked.
        countdownLatch();
    }
}
