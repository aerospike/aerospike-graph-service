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

package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.BulkLoadStateStatusMap;

import java.util.Arrays;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_IN_PROGRESS;

public class SparkBulkLoaderStatePreflight extends SparkBulkLoaderState {
    private static final int DRYRUN_STACKTRACE_LIMIT = 5;

    public SparkBulkLoaderStatePreflight(SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Preflight check
        try {
            DatasetOperations.preflightCheck(
                    sparkBulkLoaderStateMachine.edgeDataset,
                    sparkBulkLoaderStateMachine.vertexDataset,
                    sparkBulkLoaderStateMachine.config);
        } catch (final Exception e) {
            // We are limiting stacktrace size by DRYRUN_STACKTRACE_LIMIT
            StackTraceElement[] originalStackTrace = e.getStackTrace();
            StackTraceElement[] limitedStackTrace =
                    Arrays.copyOf(originalStackTrace, Math.min(originalStackTrace.length, DRYRUN_STACKTRACE_LIMIT));
            e.setStackTrace(limitedStackTrace);
            throw e;
        }
        sparkBulkLoaderStateMachine.progressBar.setPreflightCheckComplete();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStatePersistEdgeIds(sparkBulkLoaderStateMachine);
    }

    @Override
    protected BulkLoadStateStatusMap getStateMap() {
        return new BulkLoadStateStatusMap("verifying input validity", false, BULK_LOAD_STATUS_IN_PROGRESS);
    }
}
