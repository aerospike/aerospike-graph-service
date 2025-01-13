package com.aerospike.firefly.bulkloader;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderState;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateDone;
import com.aerospike.firefly.bulkloader.statemachine.states.SparkBulkLoaderStateStart;
import com.aerospike.firefly.process.call.bulkload.utils.FireflyBulkLoaderInterface;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.JOB_ALREADY_RUNNING;
import static com.aerospike.firefly.process.call.bulkload.BulkLoaderServiceLoad.BULK_LOAD_SUCCESS;
import static com.aerospike.firefly.process.call.bulkload.BulkLoaderServiceLoad.formatErrorCount;

public class SparkBulkLoaderMain implements FireflyBulkLoaderInterface {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderMain.class);

    private static final Map<String, Future<SparkBulkLoaderStateMachine>> RUNNING_JOBS = new HashMap<>();

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
        SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine = null;
        try {
            RUNNING_JOBS.put(uuid, executor.submit(() -> new SparkBulkLoaderStateMachine(args)));
            sparkBulkLoaderStateMachine = RUNNING_JOBS.get(uuid).get();
            SparkBulkLoaderState state = new SparkBulkLoaderStateStart(sparkBulkLoaderStateMachine);
            while (!(state instanceof SparkBulkLoaderStateDone)) {
                state.executeState();
                state = state.transitionState();
            }
            final String output = formatErrorCount(sparkBulkLoaderStateMachine.initializerGraph);
            if (!output.equals(BULK_LOAD_SUCCESS)) {
                LOGGER.warn(output);
            }
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
            final String message = "Initializing the bulk loader was unexpectedly interrupted. Please retry or contact support if the problem persists.";
            LOGGER.error(message);
            throw new RuntimeException(message, ie);
        } finally {
            if (sparkBulkLoaderStateMachine != null) {
                sparkBulkLoaderStateMachine.cleanup();
            }
            executor.shutdown();
            try {
                boolean shutdownSucceeded = executor.awaitTermination(10, java.util.concurrent.TimeUnit.SECONDS);
                if (!shutdownSucceeded) {
                    LOGGER.error("Failed to shutdown executor.");
                }
            } catch (final InterruptedException e) {
                LOGGER.error("Failed to shutdown executor", e);
            }
            System.clearProperty("BULK_LOADING");
            RUNNING_JOBS.remove(uuid);
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
}
