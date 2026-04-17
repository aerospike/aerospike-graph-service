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

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.BulkLoadStateStatusMap;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_IN_PROGRESS;

public class SparkBulkLoaderStateCleanUp extends SparkBulkLoaderState {
    public SparkBulkLoaderStateCleanUp(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Clean up recovery artifacts in spark and Aerospike.
        // User must cleanup their own data in HDFS.
        if (!sparkBulkLoaderStateMachine.readOnly) {
            sparkBulkLoaderStateMachine.edgeDataset.unpersist();
            RecoveryUtil.truncate(sparkBulkLoaderStateMachine.initializerGraph);
            sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph().disableBulkLoadSetIndexes();
        }
        if (sparkBulkLoaderStateMachine.incrementalLoad) {
            sparkBulkLoaderStateMachine.progressBar.latchMergeInfo();
            sparkBulkLoaderStateMachine.initializerGraph.fireflySummaryUpdater.clearMergeVertexPartitionData();
        }
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateDone(sparkBulkLoaderStateMachine);
    }

    @Override
    protected BulkLoadStateStatusMap getStateMap() {
        return new BulkLoadStateStatusMap("cleanup", false, BULK_LOAD_STATUS_IN_PROGRESS);
    }
}
