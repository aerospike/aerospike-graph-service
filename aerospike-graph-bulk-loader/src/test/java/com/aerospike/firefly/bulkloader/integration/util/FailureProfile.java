/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.bulkloader.integration.util;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.policy.WritePolicy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * This class is used in conjunction with the FailingAerospikeClient class and
 * can be used to simulate failures in Aerospike transactions without actually
 * forcing nodes to fail.
 * <p/>
 * Note that this is useful from the application level to ensure failures are
 * handled properly and accurately but does not actually cause the database to
 * fail from the database level. Hence, it cannot be used to test error recovery
 * when a node crashes especially in Strong Consistency mode.
 * <p/>
 * Failures can be done randomly using probabilities, or specifically at set
 * points in a transaction.
 *
 *
 */
public class FailureProfile {
    public enum FailureType {
        TIMEOUT, THREAD_DEATH, EXCEPTION
    }

    /**
     * A callback when a call is forced to fail. This can be used for logging, etc.
     *
     */
    public interface FailureCallback {
        public void failureOccurred(FailureType type, boolean beforeCall);
    }

    /** Chance of a read throwing a Timeout exception, range [0,1] */
    private double chanceOfReadTimeout = 0.0;
    /** Chance of a write throwing a Timeout exception, range [0,1] */
    private double chanceOfWriteTimeout = 0.0;
    /** Chance of a read throwing an Error, simulating thread death, range [0,1] */
    private double chanceOfThreadFailureOnWrites = 0.0;
    /** Chance of a write throwing an Error, simulating thread death, range [0,1] */
    private double chanceOfThreadFailureOnRead = 0.0;
    /**
     * When a timeout occurs before the operation, what is the probability of
     * inDoubt being true, rang e[0,1]
     */
    private double chanceOfPreTimeoutBeingInDoubt = 0.95;

    /** If disabled, the failure profile will just pass through */
    private volatile boolean enabled = false;

    private FailureCallback callback = null;

    public FailureProfile() {
        this(0, 0, 0, 0, null);
    }

    public FailureProfile(FailureCallback callback) {
        this(0, 0, 0, 0, callback);
    }

    public FailureProfile(double chanceOfReadTimeout, double chanceOfWriteTimeout, double chanceOfThreadFailureOnWrites,
            double chanceOfThreadFailureOnReads) {
        this(chanceOfReadTimeout, chanceOfWriteTimeout, chanceOfThreadFailureOnWrites, chanceOfThreadFailureOnReads,
                null);
    }

    public FailureProfile(double chanceOfReadTimeout, double chanceOfWriteTimeout, double chanceOfThreadFailureOnWrites,
            double chanceOfThreadFailureOnReads, FailureCallback callback) {
        super();
        this.chanceOfReadTimeout = chanceOfReadTimeout;
        this.chanceOfWriteTimeout = chanceOfWriteTimeout;
        this.chanceOfThreadFailureOnWrites = chanceOfThreadFailureOnWrites;
        this.chanceOfThreadFailureOnRead = chanceOfThreadFailureOnReads;
        this.callback = callback;
        this.enabled = chanceOfReadTimeout > 0 || chanceOfWriteTimeout > 0 || chanceOfThreadFailureOnWrites > 0 || chanceOfThreadFailureOnReads > 0;
    }

    public FailureProfile chanceOfReadTimeout(double chanceOfReadTimeout) {
        this.chanceOfReadTimeout = chanceOfReadTimeout;
        return this;
    }

    public FailureProfile chanceofThreadFailureOnReads(double chanceofThreadFailureOnReads) {
        this.chanceOfThreadFailureOnRead = chanceofThreadFailureOnReads;
        return this;
    }

    public FailureProfile chanceOfThreadFailureOnWrites(double chanceOfThreadFailureOnWrites) {
        this.chanceOfThreadFailureOnWrites = chanceOfThreadFailureOnWrites;
        return this;
    }

    public FailureProfile chanceOfWriteTimeout(double chanceOfWriteTimeout) {
        this.chanceOfWriteTimeout = chanceOfWriteTimeout;
        return this;
    }

    public FailureProfile chanceOfPreTimeoutBeingInDoubt(double chanceOfPreTimeoutBeingInDoubt) {
        this.chanceOfPreTimeoutBeingInDoubt = chanceOfPreTimeoutBeingInDoubt;
        return this;
    }

    public FailureProfile failureCallback(FailureCallback callback) {
        this.callback = callback;
        return this;
    }

    public double getChanceOfReadTimeout() {
        return chanceOfReadTimeout;
    }

    public double getChanceOfWriteTimeout() {
        return chanceOfWriteTimeout;
    }

    public double getChanceOfThreadFailureOnWrites() {
        return chanceOfThreadFailureOnWrites;
    }

    public double getChanceOfThreadFailureOnReads() {
        return chanceOfThreadFailureOnRead;
    }

    public FailureCallback getCallback() {
        return callback;
    }

    public void enable() {
        this.enabled = true;
    }

    public void disable() {
        this.enabled = false;
    }

    public boolean isEnabled() {
        return enabled;
    }

    protected void preReadTxn() {
        if (enabled) {
            applyActionsForCurrentStep();
            if (chanceOfReadTimeout > 0.0 && ThreadLocalRandom.current().nextDouble() < chanceOfReadTimeout / 2) {
                if (callback != null)
                    callback.failureOccurred(FailureType.TIMEOUT, true);
                boolean inDoubt = true;
                if (chanceOfPreTimeoutBeingInDoubt < 1) {
                    inDoubt = ThreadLocalRandom.current().nextDouble() <= chanceOfPreTimeoutBeingInDoubt;
                }
                throw new AerospikeException.Timeout("Timeout", 1, 1, inDoubt);
            }
            if (chanceOfThreadFailureOnRead > 0.0
                    && ThreadLocalRandom.current().nextDouble() < chanceOfThreadFailureOnRead / 2) {
                if (callback != null)
                    callback.failureOccurred(FailureType.THREAD_DEATH, true);
                throw new Error("Thread Death");
            }
        }
    }

    protected void postReadTxn() {
        if (enabled) {
            applyActionsForCurrentStep();
            if (chanceOfReadTimeout > 0.0 && ThreadLocalRandom.current().nextDouble() < chanceOfReadTimeout / 2) {
                if (callback != null)
                    callback.failureOccurred(FailureType.TIMEOUT, false);
                throw new AerospikeException.Timeout("Timeout", 1, 1, true);
            }
            if (chanceOfThreadFailureOnRead > 0.0
                    && ThreadLocalRandom.current().nextDouble() < chanceOfThreadFailureOnRead / 2) {
                if (callback != null)
                    callback.failureOccurred(FailureType.THREAD_DEATH, false);
                throw new Error("Thread Death");
            }
        }
    }

    protected void preWriteTxn() {
        if (enabled) {
            applyActionsForCurrentStep();
            if (chanceOfWriteTimeout > 0.0 && ThreadLocalRandom.current().nextDouble() < chanceOfWriteTimeout / 2) {
                if (callback != null)
                    callback.failureOccurred(FailureType.TIMEOUT, true);
                boolean inDoubt = true;
                if (chanceOfPreTimeoutBeingInDoubt < 1) {
                    inDoubt = ThreadLocalRandom.current().nextDouble() <= chanceOfPreTimeoutBeingInDoubt;
                }
                // Force the exception to be indoubt if required, server call.
                WritePolicy wp = new WritePolicy();
                AerospikeException ae = new AerospikeException.Timeout(wp, false);
                ae.setInDoubt(inDoubt, 2);
                ae.setIteration(2);
                throw ae;
            }
            if (chanceOfThreadFailureOnWrites > 0.0
                    && ThreadLocalRandom.current().nextDouble() < chanceOfThreadFailureOnWrites / 2) {
                if (callback != null)
                    callback.failureOccurred(FailureType.THREAD_DEATH, true);
                throw new Error("Thread Death");
            }
        }
    }

    protected void postWriteTxn() {
        if (enabled) {
            applyActionsForCurrentStep();
            if (chanceOfWriteTimeout > 0.0 && ThreadLocalRandom.current().nextDouble() < chanceOfWriteTimeout / 2) {
                if (callback != null) {
                    callback.failureOccurred(FailureType.TIMEOUT, false);
                }
                // Force the exception to be indoubt, server call.
                WritePolicy wp = new WritePolicy();
                AerospikeException ae = new AerospikeException.Timeout(wp, false);
                ae.setInDoubt(true, 2);
                ae.setIteration(2);
                throw ae;
            }
            if (chanceOfThreadFailureOnWrites > 0.0
                    && ThreadLocalRandom.current().nextDouble() < chanceOfThreadFailureOnWrites / 2) {
                if (callback != null)
                    callback.failureOccurred(FailureType.THREAD_DEATH, false);
                throw new Error("Thread Death");
            }
        }
    }

    protected static abstract class Action {
        protected final FailureCallback callback;
        protected final boolean beforeCall;

        public Action(FailureCallback callback, boolean beforeCall) {
            this.callback = callback;
            this.beforeCall = beforeCall;
        }

        public boolean isBeforeCall() {
            return beforeCall;
        }

        public abstract void perform();
    }

    protected static class FailAction extends Action {
        private final FailureType failureType;
        private final RuntimeException exception;

        public FailAction(FailureType failureType, FailureCallback callback, boolean beforeCall) {
            this(failureType, callback, beforeCall, null);
        }

        public FailAction(FailureType failureType, FailureCallback callback, boolean beforeCall,
                RuntimeException exception) {
            super(callback, beforeCall);
            this.failureType = failureType;
            this.exception = exception;
        }

        @Override
        public void perform() {
            if (this.callback != null) {
                this.callback.failureOccurred(failureType, this.beforeCall);
            }
            switch (failureType) {
            case THREAD_DEATH:
                throw new Error("Thread Death");
            case TIMEOUT:
                throw new AerospikeException.Timeout("Timeout", 1, 1, true);
            case EXCEPTION:
                throw this.exception;
            }
        }
    }

    protected static class DelayAction extends Action {
        private final long timeInMs;

        public DelayAction(long timeInMs, FailureCallback callback, boolean beforeCall) {
            super(callback, beforeCall);
            this.timeInMs = timeInMs;
        }

        @Override
        public void perform() {
            try {
                Thread.sleep(timeInMs);
            } catch (InterruptedException e) {
            }
        }
    }

    public ThreadControl threadSpecificActions() {
        return new ThreadControl(this);
    }

    private static class ThreadData {
        public Map<Integer, List<Action>> stepActions = new HashMap<>();
        public int stepNumber = 0;
    }

    private static final ThreadLocal<ThreadData> threadData = new ThreadLocal<ThreadData>() {
        @Override
        protected ThreadData initialValue() {
            return new ThreadData();
        }
    };

    private void applyActionsForCurrentStep() {
        // Get the current step and increment it.
        int currentStep = threadData.get().stepNumber++;
        List<Action> actions = threadData.get().stepActions.get(currentStep);
        if (actions != null) {
            for (Action action : actions) {
                action.perform();
            }
        }
    }

    public static class ThreadControl {

        private final FailureProfile failureProfile;

        protected ThreadControl(FailureProfile failureProfile) {
            this.failureProfile = failureProfile;
        }

        private void addToStep(int step, Action action) {
            // Convert from 1-based to 0-based offset on step
            int totalStep = (step - 1) * 2 + (action.isBeforeCall() ? 0 : 1);
            List<Action> actions = threadData.get().stepActions.get(totalStep);
            if (actions == null) {
                actions = new ArrayList<>();
                threadData.get().stepActions.put(totalStep, actions);
            }
            actions.add(action);
        }

        public ThreadControl clear() {
            ThreadData data = threadData.get();
            data.stepActions.clear();
            data.stepNumber = 0;
            return this;
        }

        public ThreadControl timeoutBeforeCall(int step) {
            addToStep(step, new FailAction(FailureType.TIMEOUT, this.failureProfile.getCallback(), true));
            return this;
        }

        public ThreadControl timeoutAfterCall(int step) {
            addToStep(step, new FailAction(FailureType.TIMEOUT, this.failureProfile.getCallback(), false));
            return this;
        }

        public ThreadControl exceptionBeforeCall(int step, RuntimeException exception) {
            addToStep(step, new FailAction(FailureType.EXCEPTION, this.failureProfile.getCallback(), true, exception));
            return this;
        }

        public ThreadControl exceptionAfterCall(int step, RuntimeException exception) {
            addToStep(step, new FailAction(FailureType.EXCEPTION, this.failureProfile.getCallback(), false, exception));
            return this;
        }

        public ThreadControl threadDeathBeforeCall(int step) {
            addToStep(step, new FailAction(FailureType.THREAD_DEATH, this.failureProfile.getCallback(), true));
            return this;
        }

        public ThreadControl threadDeathAfterCall(int step) {
            addToStep(step, new FailAction(FailureType.THREAD_DEATH, this.failureProfile.getCallback(), false));
            return this;
        }

        public ThreadControl delayBeforeCall(int step, TimeUnit unit, long delay) {
            long timeInMs = TimeUnit.MILLISECONDS.convert(delay, unit);
            return this.delayBeforeCall(step, timeInMs);
        }

        public ThreadControl delayBeforeCall(int step, long delayInMs) {
            addToStep(step, new DelayAction(delayInMs, this.failureProfile.getCallback(), true));
            return this;
        }

        public ThreadControl delayAfterCall(int step, TimeUnit unit, long delay) {
            long timeInMs = TimeUnit.MILLISECONDS.convert(delay, unit);
            return delayAfterCall(step, timeInMs);
        }

        public ThreadControl delayAfterCall(int step, long delayInMs) {
            addToStep(step, new DelayAction(delayInMs, this.failureProfile.getCallback(), false));
            return this;
        }
    }
}
