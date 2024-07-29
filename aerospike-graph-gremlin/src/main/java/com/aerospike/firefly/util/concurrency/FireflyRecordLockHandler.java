package com.aerospike.firefly.util.concurrency;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class FireflyRecordLockHandler {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyRecordLockHandler.class);
    private static final ConcurrentHashMap<Key, FireflyRecordLock> RECORD_LOCKS = new ConcurrentHashMap<>();
    private final AerospikeConnection db;
    // This needs a minor refactor if this class is to be used for other distributed lock records unrelated to MergeEdge.
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
        System.out.println(Thread.currentThread().getName() + "Create lock");
        final FireflyRecordLock lock = RECORD_LOCKS.computeIfAbsent(key, k -> new FireflyRecordLock(this, key));
        System.out.println(Thread.currentThread().getName() + "lock");
        FireflyRecordLock lockLock = lock.lock();
        System.out.println(Thread.currentThread().getName() + "lockLock");
        return lockLock;
    }

    static public class FireflyRecordLock {
        private final FireflyRecordLockHandler handler;
        private final Key key;
        private final ArrayBlockingQueue<Object> lockQueue;
        private final Timer timer;
        private TimerTask lockPoller;
        private AtomicInteger hotKeyCount = new AtomicInteger(0);
        private AtomicInteger hotKeyBackoff = new AtomicInteger(0);
        private AtomicBoolean printErrorToLog = new AtomicBoolean(true);

        private FireflyRecordLock(final FireflyRecordLockHandler handler, final Key key) {
            this.handler = handler;
            this.key = key;
            this.lockQueue = new ArrayBlockingQueue<>(1, true);
            this.timer = new Timer(key.toString(), true);
            this.startLockPoller(0);
        }

        private FireflyRecordLock lock() {
            Object lockRecord;
            try {
                System.out.println(Thread.currentThread().getName() + "lockQueue.poll - " + this.handler.lockTimeout + " milliseconds");
                lockRecord = lockQueue.poll(this.handler.lockTimeout, TimeUnit.MILLISECONDS);
            } catch (final InterruptedException e) {
                System.out.println(Thread.currentThread().getName() + "lockQueue.poll interrupted");
                Thread.currentThread().interrupt();
                lockRecord = null;
            }
            if (lockRecord == null) {
                System.out.println(Thread.currentThread().getName() + "lockQueue.poll null");
                final String message = "Timeout of " + this.handler.lockTimeout + " milliseconds exceeded when attempting to acquire record lock.";
                LOG.error(message);
                System.out.println(Thread.currentThread().getName() + "lockQueue.poll null throw");
                throw new RuntimeException(message);
            } else if (lockRecord instanceof PoisonPillRecord) {
                System.out.println(Thread.currentThread().getName() + "lockQueue.poll PoisonPillRecord");
                // Give the poison pill back to the queue to notify any other threads waiting for the lock.
                while (!this.lockQueue.offer(PoisonPillRecord.INSTANCE)) {
                    System.out.println(Thread.currentThread().getName() + "lockQueue.poll PoisonPillRecord offer");
                    this.lockQueue.poll();
                }
                System.out.println(Thread.currentThread().getName() + "lockQueue.poll PoisonPillRecord throw");
                // Recursively call for a new FireflyRecordLock that is not shut down.
                return this.handler.getLock(this.key);
            } else {
                System.out.println(Thread.currentThread().getName() + "lockQueue.poll return this");
                return this;
            }
        }

        /**
         * Releases the lock record. Calling this may release this for a different thread holding this in the edge case
         * of holding the lock for longer than the configured TTL or if misused by invoking more than once.
         */
        public void unlock() {
            try {
                this.handler.db.delete(this.key);
            } catch (final Exception e) {
                LOG.warn("Unexpected error when unlocking record lock.", e);
            }
            if (this.handler.starvationProtectionEnabled) {
                this.startLockPoller(this.handler.lockPollIntervalMillis);
            } else {
                this.startLockPoller(0);
            }
        }

        private synchronized void startLockPoller(final int startDelay) {
            if (lockPoller != null) {
                lockPoller.cancel();
                timer.purge();
            }
            final TimerTask poller = new AcquireRecordLockTask(this);
            this.lockPoller = poller;
            try {
                timer.schedule(poller, startDelay, this.handler.lockPollIntervalMillis);
            } catch (final IllegalStateException e) {
                // This shouldn't happen unless this class is misused badly in combination with the application being
                // paused, but giving a poison pill here handles things gracefully.
                LOG.warn("Record lock in illegal state - replacing it with a new one.");
                while (!this.lockQueue.offer(PoisonPillRecord.INSTANCE)) {
                    this.lockQueue.poll();
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
                    if (this.lockRecord.lockQueue.poll() != null) {
                        // We grabbed the lock record here meaning no one polling for it, so we shut this key down.
                        LOG.debug("Removing record lock with Key {} from RECORD_LOCKS table.", this.lockRecord.key);
                        try {
                            this.lockRecord.handler.db.delete(this.lockRecord.key);
                        } catch (final Exception e) {
                            LOG.warn("Unexpected error when releasing record lock after no more requests for it.", e);
                        }
                        System.out.println("Removing key from RECORD_LOCKS ");
                        RECORD_LOCKS.remove(this.lockRecord.key);
                        // Put a poison pill in the queue to notify threads that started polling this after started shut down.
                        while (!this.lockRecord.lockQueue.offer(PoisonPillRecord.INSTANCE)) {
                            this.lockRecord.lockQueue.poll();
                        }
                        this.cancel();
                        this.lockRecord.timer.cancel();
                    } else {
                        // This stops this TimerTask since we found a result - schedule a new one after TTL expires to
                        // handle application downtime.
                        this.lockRecord.startLockPoller(this.lockRecord.handler.lockTtl);
                    }
                } catch (final AerospikeException e) {
                    if (e.getResultCode() == ResultCode.KEY_BUSY) {
                        // Hot key.
                        LOG.warn("Hot key on record lock with Key {}. Backing off before next grab attempt.", this.lockRecord.key);
                        this.lockRecord.hotKeyBackoff.set(this.lockRecord.hotKeyCount.incrementAndGet());
                    } else if (e.getResultCode() != ResultCode.KEY_EXISTS_ERROR) {
                        if (this.lockRecord.printErrorToLog.getAndSet(false)) {
                            LOG.warn("Unexpected error when attempting to acquire record lock.", e);
                        }
                    }
                }
            }
        }

        static private class PoisonPillRecord {
            static private final PoisonPillRecord INSTANCE = new PoisonPillRecord();
        }
    }
}

