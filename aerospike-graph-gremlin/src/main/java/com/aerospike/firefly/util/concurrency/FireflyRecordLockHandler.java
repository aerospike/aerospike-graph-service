package com.aerospike.firefly.util.concurrency;

import com.aerospike.client.Key;
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
import java.util.concurrent.atomic.AtomicReference;

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
        final int schedulerThreads = Math.max(8, Math.min(256, Runtime.getRuntime().availableProcessors() * 2));
        SCHEDULER = new ScheduledThreadPoolExecutor(schedulerThreads, task -> {
            final Thread executorThread = new Thread(task, "firefly-lock-poller");
            executorThread.setDaemon(true);
            return executorThread;
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
        this.lockTtl = db.getConfig().mergeEdgeTtl;
        this.lockTimeout = db.getConfig().mergeEdgeEvalTimeout;
        this.lockPollIntervalMillis = db.getConfig().mergeEdgePollInterval;
        this.starvationProtectionEnabled = db.getConfig().mergeEdgeStarvationProtection;
    }

    /**
     * Acquire a record lock for {@code key}. Returns only when this JVM instance owns both:
     *  (1) local baton (FIFO) and (2) the distributed Aerospike lock.
     *
     * Caller MUST call {@link FireflyRecordLock#unlock()} exactly once.
     */
    public FireflyRecordLock getLock(final Key key) {
        final FireflyRecordLock lock = RECORD_LOCKS.compute(key, (k, existing) -> {
            final FireflyRecordLock lockForKey = existing != null ? existing : new FireflyRecordLock(this, k);
            lockForKey.pendingRequests.incrementAndGet();
            return lockForKey;
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
        private final AtomicReference<ScheduledFuture<?>> pollerRef = new AtomicReference<>();

        // Awaiters for this key.
        private final AtomicInteger pendingRequests = new AtomicInteger(0);

        // Set when distributed record written and baton offered.
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
        }

        private boolean fastPathEligible() {
            // We are the first waiter (value includes us), don't already have the distributed lock, and no baton staged.
            return pendingRequests.get() == 1
                    && !holdingDistributed.get()
                    && baton.peek() == null;
        }

        /**
         * Acquire: try inline fast-path first, then wait for baton with the remaining timeout.
         */
        private FireflyRecordLock lock() {
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
                } catch (final AerospikeGraphException age) {
                    if (age.errorCode == GraphError.NSUP_DISABLED.code) {
                        decrementAndCleanupIfLast();
                        throw age;
                    }
                    // KEY_BUSY / KEY_EXISTS_ERROR etc → fall through to waiting path.
                } catch (final Exception unexpected) {
                    LOG.error("Unexpected error acquiring immediate lock for key {}", key, unexpected);
                }
            }

            // Ensure the poller is running; it no-ops if nothing to do.
            ensurePoller();

            // Wait for baton with the remaining timeout.
            try {
                final Object token = baton.poll(handler.lockTimeout, TimeUnit.MILLISECONDS);
                if (token != null) {
                    return this;
                }
            } catch (final InterruptedException ie) {
                Thread.currentThread().interrupt();
                LOG.error("Interrupted while waiting for record lock baton for key {}", key, ie);
            }

            // Timeout: if we’re the last waiter and a baton exists, free the distributed lock early.
            if (decrementAndCleanupIfLast()) {
                final Object token = baton.poll();
                if (token != null) {
                    try {
                        handler.db.delete(key, null);
                    } catch (final Exception e) {
                        LOG.warn("Unexpected error during Best-effort delete after timeout for key {} ", key, e);
                    } finally {
                        this.holdingDistributed.set(false);
                    }
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
                        handler.db.delete(key, null, true);
                    }
                }
            } catch (final Exception e) {
                LOG.error("Unexpected error when unlocking record lock for key {}", key, e);
            } finally {
                holdingDistributed.set(false);
            }

            if (!decrementAndCleanupIfLast()) {
                // More waiters exist → restart poller to acquire and hand off baton
                final int delay = handler.starvationProtectionEnabled ? handler.lockPollIntervalMillis : 0;
                schedulePoller(delay);
            }
        }

        /** Make sure a poller is active if there are waiters and no owner. */
        private void ensurePoller() {
            final ScheduledFuture<?> currentPoller = pollerFuture();
            if (currentPoller == null || currentPoller.isCancelled()) {
                schedulePoller(0);
            }
        }

        private ScheduledFuture<?> pollerFuture() { return pollerRef.get(); }

        /** Schedule (or reschedule) the poller to acquire the distributed lock and hand off the baton. */
        private void schedulePoller(final int initialDelayMs) {
            final ScheduledFuture<?> newFut =
                    SCHEDULER.scheduleAtFixedRate(new AcquireRecordLockTask(this),
                            initialDelayMs,
                            handler.lockPollIntervalMillis,
                            TimeUnit.MILLISECONDS);
            final ScheduledFuture<?> old = pollerRef.getAndSet(newFut);
            if (old != null) {
                old.cancel(false);
            }
        }

        /** Stop polling. */
        private void cancelPoller() {
            final ScheduledFuture<?> old = pollerRef.getAndSet(null);
            if (old != null) {
                old.cancel(false);
            }
        }

        /** Decrement waiters; if this was the last, remove from map and stop the poller. */
        private boolean decrementAndCleanupIfLast() {
            final AtomicBoolean removed = new AtomicBoolean(false);
            RECORD_LOCKS.compute(this.key, (k, cur) -> {
                final int remaining = pendingRequests.decrementAndGet();
                if (remaining <= 0) {
                    this.cancelPoller();
                    removed.set(true);
                    if (cur == this) {
                        // If cur == this, we can safely remove from map.
                        return null;
                    } else {
                        // Leave the current mapping untouched because someone replaced us.
                        return cur;
                    }
                }
                return cur; // keep current instance in map.
            });
            return removed.get();
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
                    lockRecord.handler.db.writeKeyLock(lockRecord.key, lockRecord.handler.lockTtl);
                    lockRecord.holdingDistributed.set(true);
                    lockRecord.lockAcquireTime.set(System.currentTimeMillis());

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
                        int hotKeyCount = Math.min(15, lockRecord.hotKeyCount.incrementAndGet());
                        lockRecord.hotKeyBackoff.set(Math.max(1, 2 << hotKeyCount));
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
