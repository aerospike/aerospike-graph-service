package com.aerospike.firefly.bulkloader;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.process.call.bulkload.utils.FireflyBulkLoaderInterface;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

public class SparkBulkLoaderMain implements FireflyBulkLoaderInterface {

    private static final Map<String, SparkBulkLoaderStateMachine> RUNNING_JOBS = new HashMap<>();

    public static void main(final String[] args) {
        // Create new Object so we can invoke non-static method load()
        new SparkBulkLoaderMain().load(args);
    }

    public void load(final String[] args) {
        if (!RUNNING_JOBS.isEmpty()) {
            throw new IllegalStateException("Another job is already running.");
        }
        SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(args);
        final String uuid = UUID.randomUUID().toString();
        RUNNING_JOBS.put(uuid, stateMachine);
        try {
            stateMachine.executeStateMachine();
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
