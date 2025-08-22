package com.aerospike.firefly.util.concurrency;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.ResultCode;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.aerospike.firefly.util.exceptions.GraphError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Scalable distributed record lock handler.
 *
 * Semantics:
 *  - Distributed lock is acquired via db.writeKeyLock(key, ttl) and released via db.delete(key, null).
 *  - Local fairness via a baton queue (capacity=1, fair=true).
 *  - A shared scheduler polls only when needed; first waiter attempts inline before waiting.
 */
public class FireflyRecordLockHandler {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyRecordLockHandler.class);

    // All live per-key locks.
    private static final ConcurrentHashMap<Key, FireflyRecordLock> RECORD_LOCKS = new ConcurrentHashMap<>();

    // Shared scheduler for all polling tasks (daemon). Size is generous to avoid starvation under many hot keys.
    private static final ScheduledThreadPoolExecutor SCHEDULER;
    static {
        // We should not have less than 8, but also shouldn't go crazy on very large machines so cap at 256.
        int n = Math.max(8, Math.min(256, Runtime.getRuntime().availableProcessors() * 2));
        SCHEDULER = new ScheduledThreadPoolExecutor(n, r -> {
            Thread t = new Thread(r, "firefly-lock-poller");
            t.setDaemon(true);
            return t;
        });
        SCHEDULER.setRemoveOnCancelPolicy(true);
        SCHEDULER.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        SCHEDULER.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
    }

    private final AerospikeConnection db;
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
     * Acquire a record lock for {@code key}. Returns only when this JVM instance owns both:
     *  (1) local baton (FIFO) and (2) the distributed Aerospike lock.
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
        private static final Object TOKEN = new Object();

        private final FireflyRecordLockHandler handler;
        private final Key key;

        // Fair hand-off queue. One token enqueued == one rightful owner proceeds.
        private final ArrayBlockingQueue<Object> baton = new ArrayBlockingQueue<>(1, true);

        // Poller future (null when not polling).
        private volatile ScheduledFuture<?> pollerFuture;

        // Awaiters for this key.
        private final AtomicInteger pendingRequests = new AtomicInteger(0);

        // True iff this instance currently holds the distributed lock (set when baton consumed).
        private final AtomicBoolean holdingDistributed = new AtomicBoolean(false);

        // When the current owner acquired the distributed lock (ms since epoch).
        private final AtomicLong lockAcquireTime = new AtomicLong(0);

        // Exponential backoff state for hot keys.
        private final AtomicInteger hotKeyCount = new AtomicInteger(0);
        private final AtomicInteger hotKeyBackoff = new AtomicInteger(0);

        // Rate-limit unexpected error logs during contention.
        private final AtomicBoolean printErrorToLog = new AtomicBoolean(true);

        private FireflyRecordLock(final FireflyRecordLockHandler handler, final Key key) {
            this.handler = handler;
            this.key = key;
            schedulePoller(0);
        }

        private boolean fastPathEligible() {
            // We are the first waiter (value includes us), no current owner, and no baton staged.
            return pendingRequests.get() == 1
                    && !holdingDistributed.get()
                    && baton.peek() == null;
        }

        /**
         * Acquire: try inline fast-path first, then wait for baton with the remaining timeout.
         */
        private FireflyRecordLock lock() {
            final long deadlineNs = System.nanoTime() +
                    TimeUnit.MILLISECONDS.toNanos(handler.lockTimeout);

            // Try fast path only if we're the first (and only) waiter.
            if (fastPathEligible()) {
                try {
                    if (handler.db.writeKeyLock(key, handler.lockTtl) != null) {
                        holdingDistributed.set(true);
                        lockAcquireTime.set(System.currentTimeMillis());
                        // Defensive: clear any stray baton (should be null here).
                        baton.poll();
                        return this;
                    }
                } catch (AerospikeGraphException fatal) {
                    if (fatal.errorCode == GraphError.NSUP_DISABLED.code) {
                        decrementAndCleanupIfLast();
                        throw fatal;
                    }
                    // KEY_BUSY / KEY_EXISTS_ERROR etc → fall through to waiting path.
                } catch (Exception unexpected) {
                    if (printErrorToLog.getAndSet(false)) {
                        LOG.error("Unexpected inline lock error for key {}", key, unexpected);
                    }
                }
            }

            // Ensure persistent poller is running; it no-ops if nothing to do.
            ensurePoller();

            // Wait for baton with the remaining timeout.
            try {
                long remainingMs = TimeUnit.NANOSECONDS.toMillis(deadlineNs - System.nanoTime());
                if (remainingMs > 0) {
                    Object token = baton.poll(remainingMs, TimeUnit.MILLISECONDS);
                    if (token != null) {
                        holdingDistributed.set(true);
                        lockAcquireTime.set(System.currentTimeMillis());
                        return this;
                    }
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.error("Interrupted while waiting for record lock baton for key {}", key, ie);
            }

            // Timeout: if we’re the last waiter and a baton exists, free the distributed lock early.
            if (decrementAndCleanupIfLast()) {
                Object token = baton.poll();
                if (token != null) {
                    try { handler.db.delete(key, null); }
                    catch (Exception e) { LOG.warn("Best-effort delete after timeout for {}", key, e); }
                }
            }
            throw new RuntimeException("Timeout of " + handler.lockTimeout +
                    "ms exceeded when attempting to acquire record lock for key " + key + ".");
        }

        /**
         * Release: delete distributed lock (if still valid) and either wake next waiter (restart poller) or clean up.
         * Must be called exactly once by the owner.
         */
        public void unlock() {
            try {
                if (holdingDistributed.get()) {
                    long heldMs = System.currentTimeMillis() - lockAcquireTime.get();
                    if (heldMs < handler.lockTtl) {
                        handler.db.delete(key, null);
                    }
                }
            } catch (final Exception e) {
                LOG.error("Unexpected error when unlocking record lock for key {}", key, e);
            } finally {
                holdingDistributed.set(false);
            }

            final int remaining = pendingRequests.decrementAndGet();
            if (remaining == 0) {
                cleanup();
            } else {
                // More waiters exist → restart poller to acquire and hand off baton
                final int delay = handler.starvationProtectionEnabled ? handler.lockPollIntervalMillis : 0;
                schedulePoller(delay);
            }
        }

        /** Single attempt to acquire distributed lock. Returns true if successful. */
        private boolean tryAcquireDistributedOnce() {
            final Record r = handler.db.writeKeyLock(key, handler.lockTtl);
            // Success → drop a token so a waiter (if any) can proceed, but also allow inline fast-path to proceed.
            // We prefer not to enqueue the token for our own inline acquisition; just return true.
            return r != null;
        }

        /** Make sure a poller is active if there are waiters and no owner. */
        private void ensurePoller() {
            ScheduledFuture<?> f = pollerFuture;
            if (f == null || f.isCancelled()) {
                schedulePoller(0);
            }
        }

        /** Schedule (or reschedule) the poller to acquire the distributed lock and hand off the baton. */
        private void schedulePoller(final int initialDelayMs) {
            cancelPoller(); // ensure single active poller
            pollerFuture = SCHEDULER.scheduleAtFixedRate(
                    new AcquireRecordLockTask(this),
                    Math.max(0, initialDelayMs),
                    Math.max(1, handler.lockPollIntervalMillis),
                    TimeUnit.MILLISECONDS
            );
        }

        /** Stop polling. */
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
            RECORD_LOCKS.remove(this.key, this); // only remove if mapping still points to this instance
        }

        /** Decrement waiters; if this was the last, clean up and return true. */
        private boolean decrementAndCleanupIfLast() {
            if (pendingRequests.decrementAndGet() == 0) {
                cleanup();
                return true;
            }
            return false;
        }

        /**
         * Poller: backs off on hot keys, attempts writeKeyLock, and hands a baton to exactly one waiter (FIFO).
         * Cancels itself after a successful handoff until the next unlock().
         */
        private static final class AcquireRecordLockTask implements Runnable {
            private final FireflyRecordLock lockRecord;

            private AcquireRecordLockTask(final FireflyRecordLock lockRecord) {
                this.lockRecord = lockRecord;
            }

            @Override
            public void run() {
                // If no waiters, don't waste cycles.
                if (lockRecord.pendingRequests.get() <= 0 || lockRecord.holdingDistributed.get()) {
                    lockRecord.cancelPoller();
                    return;
                }

                // Hot-key backoff ticks
                if (lockRecord.hotKeyBackoff.getAndDecrement() > 0) {
                    return;
                }

                try {
                    final Record r = lockRecord.handler.db.writeKeyLock(lockRecord.key, lockRecord.handler.lockTtl);

                    // Success → hand off baton to one waiter in FIFO order
                    // If a waiter already consumed via inline fast-path, baton may not be needed; that's OK.
                    lockRecord.baton.offer(TOKEN);
                    lockRecord.printErrorToLog.set(true);
                    lockRecord.hotKeyCount.set(0);
                    lockRecord.hotKeyBackoff.set(0);

                    lockRecord.cancelPoller(); // stop until next unlock()

                } catch (final AerospikeGraphException e) {
                    if (e.errorCode == GraphError.NSUP_DISABLED.code) {
                        throw e; // fatal in this mode
                    } else if (e.errorCode == ResultCode.KEY_BUSY) {
                        // Server considers this a hot key — exponential backoff
                        LOG.warn("Hot key on record lock {}. Backing off before next attempt.", lockRecord.key);
                        int c = lockRecord.hotKeyCount.incrementAndGet();
                        if (c > 15) c = 15;
                        lockRecord.hotKeyBackoff.set(Math.max(1, 2 << c));
                    } else if (e.errorCode == ResultCode.KEY_EXISTS_ERROR) {
                        // Lock held elsewhere — quiet retry on next tick
                    } else {
                        if (lockRecord.printErrorToLog.getAndSet(false)) {
                            LOG.error("Unexpected error acquiring record lock for key {}", lockRecord.key, e);
                        }
                    }
                } catch (final Exception e) {
                    if (lockRecord.printErrorToLog.getAndSet(false)) {
                        LOG.error("Unexpected runtime error in lock poller for key {}", lockRecord.key, e);
                    }
                }
            }
        }
    }
}
