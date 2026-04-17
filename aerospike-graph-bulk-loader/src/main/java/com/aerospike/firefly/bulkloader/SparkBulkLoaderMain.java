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

package com.aerospike.firefly.bulkloader;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderState;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateDone;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateError;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateStart;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.process.call.bulkload.BulkLoaderServiceErrors;
import com.aerospike.firefly.process.call.bulkload.utils.FireflyBulkLoaderInterface;
import com.aerospike.firefly.structure.FireflyGraph;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.spark.SparkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicReference;

import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.JOB_ALREADY_RUNNING;

public class SparkBulkLoaderMain implements FireflyBulkLoaderInterface {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderMain.class);

    public static final String BULK_LOAD_SUCCESS = "Success";
    private static final Map<String, Future<SparkBulkLoaderStateMachine>> RUNNING_JOBS = new HashMap<>();
    private static final AtomicReference<SparkBulkLoaderState> CURRENT_STATE = new AtomicReference<>();

    public static void main(final String[] args) {
        // Create new Object so we can invoke non-static method load()
        new SparkBulkLoaderMain().load(args);
    }

    public void load(final String[] args) {
        if (!RUNNING_JOBS.isEmpty()) {
            throw new IllegalStateException(JOB_ALREADY_RUNNING);
        }
        System.setProperty("BULK_LOADING", "true");

        // Make as daemon so it doesn't hang L3.
        final ExecutorService executor = Executors.newSingleThreadExecutor(
                new ThreadFactoryBuilder().setDaemon(true).build());
        final String uuid = UUID.randomUUID().toString();
        RUNNING_JOBS.put(uuid, executor.submit(() -> new SparkBulkLoaderStateMachine(args)));
        SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine = null;
        boolean stateMachineInitialized = false;
        try {
            sparkBulkLoaderStateMachine = RUNNING_JOBS.get(uuid).get();
            stateMachineInitialized = true;
        } catch (final ExecutionException ee) {
            // Drill into exception and throw the root exception. Should be a RuntimeException generally.
            Throwable cause = ee;
            while (cause.getCause() != null) {
                cause = cause.getCause();
            }
            if (cause instanceof ClassNotFoundException) {
                throw new IllegalStateException("ERROR: To use the bulk loader via the call API, " +
                        "use the docker image with bulk loader support.", ee);
            }

            LOGGER.error("Failed to bootstrap SparkBulkLoaderStateMachine.", cause);
            throw (cause instanceof RuntimeException) ? (RuntimeException) cause : new RuntimeException(cause);
        } catch (final InterruptedException ie) {
            final String message = "Initializing the bulk loader was interrupted. This is most likely caused by the" +
                    " specified configuration requiring more time. Please retry using the '.with(\"evaluationTimeout\")'" +
                    " traversal modifier or contact support if the problem persists.";
            LOGGER.error(message);
            throw new RuntimeException(message, ie);
        } finally {
            if (!stateMachineInitialized) {
                cleanup(sparkBulkLoaderStateMachine, uuid);
                shutdownExecutor(executor);
            } else if (!sparkBulkLoaderStateMachine.isL2Mode) {
                shutdownExecutor(executor);
            }
        }
        if (sparkBulkLoaderStateMachine.isL2Mode) {
            loadL2Async(sparkBulkLoaderStateMachine, uuid, executor);
        } else {
            loadL3Sync(sparkBulkLoaderStateMachine, uuid);
        }
    }

    private void loadL3Sync(final SparkBulkLoaderStateMachine stateMachine, final String uuid) {
        try {
            SparkBulkLoaderState state = new SparkBulkLoaderStateStart(stateMachine);
            while (!(state instanceof SparkBulkLoaderStateDone)) {
                state.executeState();
                state = state.transitionState();
            }
            stateMachine.progressBar.printProgress();
            final String output = formatErrorCount(stateMachine.initializerGraph);
            if (!output.equals(BULK_LOAD_SUCCESS)) {
                LOGGER.warn(output);
            }
        } catch (final Exception e) {
            if (e instanceof SparkException && e.getCause() instanceof IllegalArgumentException
                    && e.getCause().getMessage().startsWith("CSV header does not conform to the schema.")) {
                throw new RuntimeException("Elements of different types should be in separate folders.\n\n"
                        + e.getCause().getMessage());
            }
            throw e;
        } finally {
            cleanup(stateMachine, uuid);
        }
    }

    private void loadL2Async(final SparkBulkLoaderStateMachine stateMachine, final String uuid,
                             final ExecutorService executor) {
        final SparkBulkLoaderState startingState = new SparkBulkLoaderStateStart(stateMachine);
        CURRENT_STATE.set(startingState);
        executor.submit(() -> {
            SparkBulkLoaderState state = null;
            try {
                state = startingState;
                try {
                    while (!(state instanceof SparkBulkLoaderStateDone)) {
                        state.executeState();
                        state = state.transitionState();
                        if (!(state instanceof SparkBulkLoaderStateDone)) {
                            CURRENT_STATE.set(state);
                        }
                    }
                } catch (final Exception e) {
                    Exception ex = e;
                    if (e instanceof SparkException && e.getCause() instanceof IllegalArgumentException
                            && e.getCause().getMessage().startsWith("CSV header does not conform to the schema.")) {
                        ex = new RuntimeException("Elements of different types should be in separate folders.\n\n"
                                + e.getCause().getMessage());
                    }

                    LOGGER.error("L2 bulk load failed", ex);
                    state = new SparkBulkLoaderStateError(stateMachine, ex);
                }
                stateMachine.progressBar.printProgress();
            } finally {
                cleanup(stateMachine, uuid);
                // We put this after cleanup (No RUNNING_JOBS) for Done and Error states since they mark the bulk load as complete
                CURRENT_STATE.set(state);
                executor.shutdown();
            }
        });
    }

    private void cleanup(final SparkBulkLoaderStateMachine stateMachine, final String uuid) {
        if (stateMachine != null) {
            stateMachine.cleanup();
        }
        System.clearProperty("BULK_LOADING");
        RUNNING_JOBS.remove(uuid);
    }

    private void shutdownExecutor(final ExecutorService executor) {
        executor.shutdown();
        try {
            boolean shutdownSucceeded = executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
            if (!shutdownSucceeded) {
                LOGGER.error("Failed to shutdown executor.");
            }
        } catch (final InterruptedException e) {
            LOGGER.error("Failed to shutdown executor", e);
        }
    }

    public static String formatErrorCount(final FireflyGraph graph) {
        final AerospikeConnection db = graph.getBaseGraph();
        final long badEntryCount = db.incrementAndGetBadEntryCount(0);
        final long duplicateVertexIdCount = db.incrementAndGetDuplicateVertexIdCount(0);
        final long badEdgeCount = db.incrementAndGetBadEdgeCount(0);
        if (badEntryCount == 0 && duplicateVertexIdCount == 0 && badEdgeCount == 0) {
            return BULK_LOAD_SUCCESS;
        } else {
            final String errorCount = "Warning: Errors were encountered during bulk loading." +
                    "\n\t\tduplicate-vertex-id-count: " + duplicateVertexIdCount +
                    "\n\t\tbad-edge-count: " + badEdgeCount +
                    "\n\t\tbad-entry-count: " + badEntryCount +
                    "\n\t\tUse the g.call(\"" + new BulkLoaderServiceErrors<>(graph).getName() + "\") command for details.";
            return errorCount;
        }
    }

    public static void exponentialBackoff(final int attempt) {
        // This is to prevent overflow since we cap at 10000ms anyway.
        final int cappedAttempt = Math.min(attempt, 14);

        int exponentialTime = Math.min(10000, (int) Math.pow(2, cappedAttempt));
        final int tenPercentSeed = exponentialTime / 10;
        final int jitter = (int) ((Math.random() * tenPercentSeed) - tenPercentSeed);
        exponentialTime += jitter;

        try {
            Thread.sleep(exponentialTime);
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }

    public Map<String, Object> getStatus() {
        final SparkBulkLoaderState state = CURRENT_STATE.get();
        if (state == null) {
            throw new IllegalStateException("No bulk loading jobs have been started");
        }
        return state.getStateStatus();
    }
}
