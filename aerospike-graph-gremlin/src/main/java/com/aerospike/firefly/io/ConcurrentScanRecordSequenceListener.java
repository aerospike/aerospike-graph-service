package com.aerospike.firefly.io;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.async.Monitor;
import com.aerospike.client.listener.RecordSequenceListener;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

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
    private final BiFunction<Long, Long, Void> metricsCallback;
    private long startTime = -1;

    /**
     * ConcurrentScanRecordSequenceListener will return an iterator immediately, while still receiving results.
     * The iterator will block on hasNext if no results are available until something comes in, or the query is finished.
     *
     * @param scanMonitor Aerospike scanMonitor
     * @param maxWaitMs   max time to block waiting for new events
     */
    private ConcurrentScanRecordSequenceListener(final Monitor scanMonitor,
                                                 final int maxWaitMs,
                                                 final BiFunction<Long, Long, Void> metricsCallback) {
        this.metricsCallback = metricsCallback;
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
     * create new ConcurrentScanRecordSequenceListener
     * @param db AerospikeConnection instance
     * @param scanMonitor Aerospike scanMonitor
     * @param scanId scanId
     * @return new ConcurrentScanRecordSequenceListener
     */
    public static ConcurrentScanRecordSequenceListener create(final AerospikeConnection db,
                                                              final Monitor scanMonitor,
                                                              final UUID scanId) {
        final BiFunction<Long, Long, Void> metricsCallback = (start, stop) -> {
            db.getScanHitCounter().setScanTimings(scanId, start, stop);
            return null;
        };
        return new ConcurrentScanRecordSequenceListener(scanMonitor,
                Integer.parseInt(ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.SCAN_MAX_WAIT, db.conf)),
                metricsCallback);
    }

    /**
     * Sets the start time of the scan
     */
    public void setStartTime() {
        if (this.startTime != -1) {
            throw new RuntimeException("Failed to set start time of scan monitor because it was already started.");
        }
        this.startTime = System.nanoTime();
    }

    /**
     * Will be called automatically by Aerospike as results come in
     *
     * @param key    unique record identifier
     * @param record record instance, will be null if the key is not found
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
        metricsCallback.apply(startTime, System.nanoTime());
        scanMonitor.notifyComplete();
    }

    /**
     * Triggered when scan fails
     */
    public void onFailure(final AerospikeException e) {
        // Only log an error if the scan listener was not already closed.
        if (!isClosed.get()) {
            LOG.error("Error: scan failed with exception", e);
        }
        this.complete.set(true);
        semaphore.release();
        metricsCallback.apply(startTime, System.nanoTime());
        scanMonitor.notifyComplete();
    }

    /**
     * Returns a concurrent Iterator that blocks on hasNext if no results are available
     *
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
                    metricsCallback.apply(startTime, System.nanoTime());
                    return results.take();
                } catch (InterruptedException e) {
                    terminateScan();
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public void terminateScan() {
        metricsCallback.apply(startTime, System.nanoTime());
        isClosed.set(true);
    }
}