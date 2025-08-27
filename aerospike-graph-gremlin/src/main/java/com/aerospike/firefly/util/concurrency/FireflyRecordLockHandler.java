package com.aerospike.firefly.util.concurrency;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.aerospike.firefly.util.exceptions.GraphError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class FireflyRecordLockHandler {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyRecordLockHandler.class);
    private static final ConcurrentHashMap<Key, FireflyRecordLock> RECORD_LOCKS = new ConcurrentHashMap<>();
    private final AerospikeConnection db;
    // TODO: This needs a minor refactor if this class is to be used for other distributed lock records unrelated to MergeEdge.
    private final int lockTtl;
    private final int lockTimeout;
    private final int lockPollIntervalMillis;
    private final boolean starvationProtectionEnabled;

    public FireflyRecordLockHandler(final AerospikeConnection db) {
        this.db = db;
        this.lockTtl = db.MERGE_EDGE_TTL;
        this.lockTimeout = db.MERGE_EDGE_EVAL_TIMEOUT;
        this.lockPollIntervalMillis = db.MERGE_EDGE_POLL_INTERVAL;
        this.starvationProtectionEnabled = db.MERGE_EDGE_STARVATION_PROTECTION;
    }

    /**
     * Get a record lock for a specific key. The lock is only valid for the duration of the configured TTL of the
     * lock record. Invoke FireflyRecordLock.unlock() once and only once when it is no longer needed.
     * @param key Key for the record lock.
     * @return FireflyRecordLock, which represents the current holding of the record lock.
     */
    public FireflyRecordLock getLock(final Key key) {
        final FireflyRecordLock lock;
        synchronized (RECORD_LOCKS) {
            lock = RECORD_LOCKS.computeIfAbsent(key, k -> new FireflyRecordLock(this, key));
            lock.pendingRequests.incrementAndGet();
        }
        return lock.lock();
    }

    static public class FireflyRecordLock {
        private final FireflyRecordLockHandler handler;
        private final Key key;
        private final ArrayBlockingQueue<Object> lockQueue;
        private final Timer timer;
        private final AtomicInteger pendingRequests = new AtomicInteger(0);
        private final AtomicLong lockAcquireTime = new AtomicLong(0);
        private final AtomicInteger hotKeyCount = new AtomicInteger(0);
        private final AtomicInteger hotKeyBackoff = new AtomicInteger(0);
        private final AtomicBoolean printErrorToLog = new AtomicBoolean(true);

        private FireflyRecordLock(final FireflyRecordLockHandler handler, final Key key) {
            this.handler = handler;
            this.key = key;
            this.lockQueue = new ArrayBlockingQueue<>(1, true);
            this.timer = new Timer(true);
            this.startLockPoller(0);
        }

        private FireflyRecordLock lock() {
            try {
                if (lockQueue.poll(this.handler.lockTimeout, TimeUnit.MILLISECONDS) != null) {
                    this.lockAcquireTime.set(System.currentTimeMillis());
                    return this;
                }
            } catch (final InterruptedException e) {
                // This shouldn't happen normally.
                LOG.error("Unexpected error: interrupted when trying to acquire record lock. Contact support for additional help if needed.", e);
            }
            synchronized (RECORD_LOCKS) {
                if (pendingRequests.decrementAndGet() == 0) {
                    RECORD_LOCKS.remove(this.key);
                    this.timer.cancel();
                }
            }
            final String message = "Timeout of " + this.handler.lockTimeout + " milliseconds exceeded when attempting to acquire record lock.";
            LOG.error(message);
            throw new RuntimeException(message);
        }

        /**
         * Releases the lock record. Calling this may release this for a different thread holding this in the edge case
         * of holding the lock for longer than the configured TTL or if misused by invoking more than once.
         */
        public void unlock() {
            final int pendingLockRequests;
            synchronized (RECORD_LOCKS) {
                pendingLockRequests = pendingRequests.decrementAndGet();
                if (pendingLockRequests == 0) {
                    RECORD_LOCKS.remove(this.key);
                    this.timer.cancel();
                }
            }

            try {
                if (System.currentTimeMillis() - this.lockAcquireTime.get() < this.handler.lockTtl) {
                    // Holding a record lock for longer than its TTL invalidates it since it is released, so check here
                    // to at least not unlock a different Firefly that might've grabbed this lock.
                    this.handler.db.delete(this.key, null, true);
                }
            } catch (final Exception e) {
                LOG.error("Unexpected error when unlocking record lock.", e);
            }

            if (pendingLockRequests != 0) {
                this.startLockPoller(this.handler.starvationProtectionEnabled ? this.handler.lockPollIntervalMillis : 0);
            }
        }

        private void startLockPoller(final int startDelay) {
            final TimerTask poller = new AcquireRecordLockTask(this);
            try {
                timer.schedule(poller, startDelay, this.handler.lockPollIntervalMillis);
            } catch (final IllegalStateException e) {
                // This should never happen.
                LOG.error("Unexpected error: record lock in illegal state. Contact support for additional help if needed.", e);
                synchronized (RECORD_LOCKS) {
                    RECORD_LOCKS.remove(this.key);
                    this.timer.cancel();
                }
            }
        }

        static private class AcquireRecordLockTask extends TimerTask {
            private final FireflyRecordLock lockRecord;

            private AcquireRecordLockTask(final FireflyRecordLock lockRecord) {
                this.lockRecord = lockRecord;
            }

            @Override
            public void run() {
                if (this.lockRecord.hotKeyBackoff.getAndDecrement() > 0) {
                    // Skip this polling interval to backoff.
                    return;
                }
                try {
                    final Record record = this.lockRecord.handler.db.writeKeyLock(this.lockRecord.key,
                            this.lockRecord.handler.lockTtl);
                    this.lockRecord.lockQueue.offer(record);
                    this.lockRecord.printErrorToLog.set(true);
                    this.lockRecord.hotKeyCount.set(0);
                    this.lockRecord.hotKeyBackoff.set(0);
                    this.cancel();
                } catch (final AerospikeGraphException e) {
                    if (e.errorCode == GraphError.NSUP_DISABLED.code) {
                        throw e;
                    } else if (e.errorCode == ResultCode.KEY_BUSY) {
                        // Hot key.
                        LOG.warn("Hot key on record lock with Key {}. Exponentially backing off before next grab attempt.",
                                this.lockRecord.key);
                        int hotKeyCount = this.lockRecord.hotKeyCount.getAndIncrement();
                        // Prevent overflow.
                        if (hotKeyCount > 15) {
                            hotKeyCount = 15;
                        }
                        this.lockRecord.hotKeyBackoff.set(Math.max(1, 2 << hotKeyCount));
                    } else if (e.errorCode != ResultCode.KEY_EXISTS_ERROR) {
                        if (this.lockRecord.printErrorToLog.getAndSet(false)) {
                            LOG.error("Unexpected error when attempting to acquire record lock.", e);
                        }
                    }
                }
            }
        }
    }
}

