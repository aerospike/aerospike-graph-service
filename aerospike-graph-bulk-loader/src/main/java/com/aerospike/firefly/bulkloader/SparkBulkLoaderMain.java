package com.aerospike.firefly.bulkloader;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.process.call.bulkload.utils.FireflyBulkLoaderInterface;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.JOB_ALREADY_RUNNING;

public class SparkBulkLoaderMain implements FireflyBulkLoaderInterface {

    private static final Map<String, Future<SparkBulkLoaderStateMachine>> RUNNING_JOBS = new HashMap<>();

    public static void main(final String[] args) {
        // Create new Object so we can invoke non-static method load()
        new SparkBulkLoaderMain().load(args);
    }

    public void load(final String[] args) {
        if (!RUNNING_JOBS.isEmpty()) {
            throw new IllegalStateException(JOB_ALREADY_RUNNING);
        }
        final ExecutorService executor = Executors.newSingleThreadExecutor();
        final String uuid = UUID.randomUUID().toString();
        RUNNING_JOBS.put(uuid, executor.submit(() -> new SparkBulkLoaderStateMachine(args)));
        try {
            final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine = RUNNING_JOBS.get(uuid).get();
            sparkBulkLoaderStateMachine.executeStateMachine();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
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
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
