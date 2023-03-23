package com.aerospike.firefly.io;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.query.KeyRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */

public class ConcurrentScanRecordSequenceListener implements RecordSequenceListener {
    private final Monitor scanMonitor;
    private final LinkedBlockingQueue<KeyRecord> results = new LinkedBlockingQueue<>();
    private final Semaphore semaphore;
    private final AtomicBoolean complete = new AtomicBoolean(false);
    private final int maxWaitMs;
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final Logger LOG = LoggerFactory.getLogger(ConcurrentScanRecordSequenceListener.class);

    /**
     * ConcurrentScanRecordSequenceListener will return an iterator immediately, while still receiving results.
     * The iterator will block on hasNext if no results are available until something comes in, or the query is finished.
     * @param scanMonitor Aerospike scanMonitor
     * @param maxWaitMs max time to block waiting for new events
     */
    public ConcurrentScanRecordSequenceListener(final Monitor scanMonitor,
                                                final int maxWaitMs) {
        this.scanMonitor = scanMonitor;
        this.semaphore = new Semaphore(1);
        this.maxWaitMs = maxWaitMs;
        try {
            this.semaphore.acquire();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Will be called automatically by Aerospike as results come in
     * @param key					unique record identifier
     * @param record				record instance, will be null if the key is not found
     * @throws AerospikeException
     */
    public void onRecord(final Key key, final Record record) throws AerospikeException {
        if (isClosed.get()) {
            throw new AerospikeException.ScanTerminated();
        }
        results.add(new KeyRecord(key, record));
        semaphore.release();
    }

    /**
     * Triggered when scan completes successfully
     */
    public void onSuccess() {
        this.complete.set(true);
        semaphore.release();
        scanMonitor.notifyComplete();
    }

    /**
     * Triggered when scan fails
     */
    public void onFailure(final AerospikeException e) {
        LOG.error("Error: scan failed with exception", e);
        this.complete.set(true);
        semaphore.release();
        scanMonitor.notifyComplete();
    }

    /**
     * Returns a concurrent Iterator that blocks on hasNext if no results are available
     * @return iterator
     */
    public Iterator<KeyRecord> iterator() {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                if (results.size() > 0) {
                    return true;
                }

                while (!complete.get() && results.size() == 0) {
                    try {
                        if (!semaphore.tryAcquire(maxWaitMs, TimeUnit.MILLISECONDS)) {
                            terminateScan();
                            throw new RuntimeException("timeout exceeded waiting for new records");
                        }
                    } catch (InterruptedException e) {
                        terminateScan();
                        throw new RuntimeException(e);
                    }
                }

                return results.size() > 0;
            }

            @Override
            public KeyRecord next() {
                try {
                    if (results.size() == 0) {
                        if (!hasNext()) {
                            throw new NoSuchElementException();
                        }
                    }
                    return results.take();
                } catch (InterruptedException e) {
                    terminateScan();
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public void terminateScan() {
        isClosed.set(true);
    }
}
