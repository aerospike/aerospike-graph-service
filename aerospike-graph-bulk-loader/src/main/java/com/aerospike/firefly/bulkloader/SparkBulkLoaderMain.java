package com.aerospike.firefly.bulkloader;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.process.call.bulkload.utils.FireflyBulkLoaderInterface;

public class SparkBulkLoaderMain implements FireflyBulkLoaderInterface {

    public static void main(final String[] args) {
        // Create new Object so we can invoke non-static method load()
        new SparkBulkLoaderMain().load(args);
    }

    public void load(final String[] args) {
        SparkBulkLoaderStateMachine stateMachine = new SparkBulkLoaderStateMachine(args);
        stateMachine.executeStateMachine();
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
