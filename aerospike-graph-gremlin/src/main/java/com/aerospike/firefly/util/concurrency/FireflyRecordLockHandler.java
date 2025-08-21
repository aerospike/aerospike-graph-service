package com.aerospike.firefly.util.concurrency;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.aerospike.firefly.util.exceptions.GraphError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * Distributed record lock handler:
 * - Local fairness: baton queue (capacity 1, fair=true) hands off the right to proceed.
 * - Distributed lock: db.writeKeyLock(key, ttl) acquires; db.delete(key, null) releases.
 * - A shared scheduler polls to acquire the distributed lock and deposits the baton for one waiter.
 * - No global synchronized on the lock table; uses CHM.compute for concurrency.
 *
 * Assumes lock TTL and timeout are expressed in milliseconds (as in the original).
 * If your Aerospike TTL is in seconds, convert in the AerospikeConnection wrapper.
 */
public class FireflyRecordLockHandler {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyRecordLockHandler.class);

    /** All live per-key locks. */
    private static final ConcurrentHashMap<Key, FireflyRecordLock> RECORD_LOCKS = new ConcurrentHashMap<>();

    /** Shared scheduler for all polling tasks (daemon threads). */
    private static final ScheduledExecutorService SCHEDULER =
            Executors.newScheduledThreadPool(
                    Math.max(2, Runtime.getRuntime().availableProcessors() / 4),
                    new ThreadFactory() {
                        private final AtomicInteger n = new AtomicInteger();
                        @Override public Thread newThread(Runnable r) {
                            Thread t = new Thread(r, "firefly-lock-poller-" + n.incrementAndGet());
                            t.setDaemon(true);
                            return t;
                        }
                    });

    private final AerospikeConnection db;
    private final int lockTtl;                 // ms
    private final int lockTimeout;             // ms
    private final int lockPollIntervalMillis;  // ms
    private final boolean starvationProtectionEnabled;

    public FireflyRecordLockHandler(final AerospikeConnection db) {
        this.db = Objects.requireNonNull(db, "db");
        this.lockTtl = db.MERGE_EDGE_TTL;
        this.lockTimeout = db.MERGE_EDGE_EVAL_TIMEOUT;
        this.lockPollIntervalMillis = db.MERGE_EDGE_POLL_INTERVAL;
        this.starvationProtectionEnabled = db.MERGE_EDGE_STARVATION_PROTECTION;
    }

    /**
     * Acquire a record lock for {@code key}. Returns only when both:
     *  1) this JVM instance is the selected waiter (baton received), and
     *  2) the distributed Aerospike lock has been acquired (writeKeyLock succeeded).
     *
     * Caller MUST call {@link FireflyRecordLock#unlock()} exactly once.
     */
    public FireflyRecordLock getLock(final Key key) {
        final FireflyRecordLock lock = RECORD_LOCKS.compute(key, (k, existing) -> {
            final FireflyRecordLock l = existing != null ? existing : new FireflyRecordLock(this, k);
            l.pendingRequests.incrementAndGet();
            return l;
        });
        return lock.lock();
    }

    /** Represents the held lock for a specific record key. */
    public static final class FireflyRecordLock {
        private final FireflyRecordLockHandler handler;
        private final Key key;

        /** Fair hand-off queue. One object enqueued == one rightful owner proceeds. */
        private final ArrayBlockingQueue<Object> baton = new ArrayBlockingQueue<>(1, true);

        /** Scheduler future for the poller (null when not polling). */
        private volatile ScheduledFuture<?> pollerFuture;

        /** Awaiters for this key. */
        private final AtomicInteger pendingRequests = new AtomicInteger(0);

        /** When the current owner acquired the distributed lock (ms since epoch). */
        private final AtomicLong lockAcquireTime = new AtomicLong(0);

        /** Exponential backoff state for hot keys. */
        private final AtomicInteger hotKeyCount = new AtomicInteger(0);
        private final AtomicInteger hotKeyBackoff = new AtomicInteger(0);

        /** Rate-limit unexpected error logs during contention. */
        private final AtomicBoolean printErrorToLog = new AtomicBoolean(true);

        /** Marker object used as a baton token. */
        private static final Object TOKEN = new Object();

        private FireflyRecordLock(final FireflyRecordLockHandler handler, final Key key) {
            this.handler = handler;
            this.key = key;
            schedulePoller(0); // immediate first attempt
        }

        /**
         * Blocks until baton is received (i.e., distributed lock acquired by the poller) or timeout elapses.
         */
        private FireflyRecordLock lock() {
            try {
                final Object got = baton.poll(handler.lockTimeout, TimeUnit.MILLISECONDS);
                if (got != null) {
                    // Distributed lock is acquired by this instance. Mark time and proceed.
                    lockAcquireTime.set(System.currentTimeMillis());
                    return this;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.error("Interrupted while waiting for record lock baton for key {}", key, e);
            }

            // Timed out or interrupted: decrement and possibly clean up.
            if (pendingRequests.decrementAndGet() == 0) {
                cleanup();
            }
            throw new RuntimeException("Timeout of " + handler.lockTimeout +
                    "ms exceeded when attempting to acquire record lock for key " + key + ".");
        }

        /**
         * Releases the distributed lock (if still valid) and hands over to the next waiter if any.
         * Must be called exactly once by the current owner.
         */
        public void unlock() {
            final int remaining;
            try {
                // Only delete if still within TTL to avoid deleting a lock renewed by someone else.
                final long heldMs = System.currentTimeMillis() - lockAcquireTime.get();
                if (heldMs < handler.lockTtl) {
                    handler.db.delete(key, null);
                }
            } catch (Exception e) {
                LOG.error("Unexpected error when unlocking record lock for key {}", key, e);
            } finally {
                remaining = pendingRequests.decrementAndGet();
            }

            if (remaining == 0) {
                cleanup();
            } else {
                // More waiters exist → re-start poller to acquire again and pass the baton.
                final int delay = handler.starvationProtectionEnabled ? handler.lockPollIntervalMillis : 0;
                schedulePoller(delay);
            }
        }

        /** Schedule (or reschedule) the poller to acquire the distributed lock and hand off the baton. */
        private void schedulePoller(final int initialDelayMs) {
            cancelPoller(); // ensure only one active poller
            pollerFuture = SCHEDULER.scheduleAtFixedRate(
                    new AcquireRecordLockTask(this),
                    Math.max(0, initialDelayMs),
                    Math.max(1, handler.lockPollIntervalMillis),
                    TimeUnit.MILLISECONDS
            );
        }

        /** Stop polling and allow GC/removal. */
        private void cancelPoller() {
            final ScheduledFuture<?> f = pollerFuture;
            if (f != null) {
                f.cancel(false);
                pollerFuture = null;
            }
        }

        /** Remove from map if no waiters; stop the poller. */
        private void cleanup() {
            cancelPoller();
            // Remove only if mapping still points to this instance
            RECORD_LOCKS.remove(this.key, this);
        }

        /**
         * Background task:
         *  - Optionally backs off when {@code KEY_BUSY} (hot key) was observed.
         *  - Attempts db.writeKeyLock(key, ttl).
         *  - On success, deposits a baton token to wake exactly one waiter (FIFO).
         *  - Cancels itself until the current owner calls unlock().
         */
        private static final class AcquireRecordLockTask implements Runnable {
            private final FireflyRecordLock lockRecord;

            private AcquireRecordLockTask(final FireflyRecordLock lockRecord) {
                this.lockRecord = lockRecord;
            }

            @Override
            public void run() {
                // If there are no pending waiters, stop polling early.
                if (lockRecord.pendingRequests.get() <= 0) {
                    lockRecord.cancelPoller();
                    return;
                }

                // Backoff ticks for hot keys
                if (lockRecord.hotKeyBackoff.getAndDecrement() > 0) {
                    return; // skip this interval
                }

                try {
                    // Attempt to acquire the distributed lock
                    final Record record = lockRecord.handler.db.writeKeyLock(
                            lockRecord.key, lockRecord.handler.lockTtl);

                    // Success → hand off baton to one waiter in FIFO order
                    final boolean offered = lockRecord.baton.offer(TOKEN);
                    // Reset hot-key & error-suppression state
                    lockRecord.printErrorToLog.set(true);
                    lockRecord.hotKeyCount.set(0);
                    lockRecord.hotKeyBackoff.set(0);

                    // Once baton is delivered, we can stop polling until unlock()
                    // (If 'offered' is false, baton was already present; still stop polling.)
                    lockRecord.cancelPoller();

                } catch (final AerospikeGraphException e) {
                    if (e.errorCode == GraphError.NSUP_DISABLED.code) {
                        // Non-recoverable in this mode → surface
                        throw e;
                    } else if (e.errorCode == ResultCode.KEY_BUSY) {
                        // Hot key (server busy) → exponential backoff
                        LOG.warn("Hot key on record lock {}. Backing off before next attempt.", lockRecord.key);
                        int c = lockRecord.hotKeyCount.incrementAndGet();
                        if (c > 15) c = 15; // cap to avoid overflow
                        // 2 << c == 2^(c+1)
                        lockRecord.hotKeyBackoff.set(Math.max(1, 2 << c));
                    } else if (e.errorCode == ResultCode.KEY_EXISTS_ERROR) {
                        // Lock held by someone else → no log spam, keep polling
                        // (Leave backoff at 0 so we retry on next tick.)
                    } else {
                        // Unexpected error: log once per burst, then keep trying
                        if (lockRecord.printErrorToLog.getAndSet(false)) {
                            LOG.error("Unexpected error acquiring record lock for key {}", lockRecord.key, e);
                        }
                    }
                } catch (final Exception e) {
                    // Defensive: unexpected runtime exception
                    if (lockRecord.printErrorToLog.getAndSet(false)) {
                        LOG.error("Unexpected runtime error in lock poller for key {}", lockRecord.key, e);
                    }
                }
            }
        }
    }
}
