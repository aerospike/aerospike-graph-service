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
import com.aerospike.firefly.bulkloader.spark.VertexOperations;
import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.BulkLoadStateStatusMap;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import com.aerospike.firefly.runtime.tasks.FireflyGraphSummaryUpdater;
import org.apache.spark.sql.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_IN_PROGRESS;

public class SparkBulkLoaderStateReadVertices extends SparkBulkLoaderState {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderStateReadVertices.class);

    SparkBulkLoaderStateReadVertices(SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Load vertex dataset.
        sparkBulkLoaderStateMachine.vertexOperations = new VertexOperations(
                sparkBulkLoaderStateMachine.config,
                sparkBulkLoaderStateMachine.vertexDirectories);
        sparkBulkLoaderStateMachine.vertexDataset = DatasetOperations.loadDataset(
                sparkBulkLoaderStateMachine.spark,
                sparkBulkLoaderStateMachine.vertexDirectories,
                VertexOperations.REQUIRED_VERTEX_HEADERS,
                DatasetOperations.getDfStorageLevel(sparkBulkLoaderStateMachine.config));
        sparkBulkLoaderStateMachine.vertexCount = sparkBulkLoaderStateMachine.vertexDataset.count();
        sparkBulkLoaderStateMachine.progressBar.setVertexTotalCount(sparkBulkLoaderStateMachine.vertexCount);


        // Latch info for incremental load.
        RecoveryUtil.writeIsIncrementalLoad(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), sparkBulkLoaderStateMachine.incrementalLoad);
        if (sparkBulkLoaderStateMachine.incrementalLoad) {
            // Get summary and set initial values for incremental load.
            FireflyGraphSummaryUpdater.FireflyElementMetadata summary =
                    sparkBulkLoaderStateMachine.initializerGraph.fireflySummaryUpdater.getFireflyStatistics();
            RecoveryUtil.writeIncrementalLoadVertexStartCount(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), summary.totalVertexCount());
            RecoveryUtil.writeIncrementalLoadEdgeStartCount(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), summary.totalEdgeCount());
        }

        // This stuff is done in the edge cache generation state, so we need to do it here if we are not generating the edge caches.
        if (!sparkBulkLoaderStateMachine.isEdgeCacheWrittenWithVertex) {
            // Assign partition count.
            sparkBulkLoaderStateMachine.vertexPartitionCount = sparkBulkLoaderStateMachine.vertexDataset.rdd().partitions().length;
            sparkBulkLoaderStateMachine.progressBar.setVertexPartitionCount(sparkBulkLoaderStateMachine.vertexPartitionCount);
            if (!sparkBulkLoaderStateMachine.readOnly) {
                // If not read-only, need to repartition the vertex dataset for consistency.
                sparkBulkLoaderStateMachine.vertexDataset = sparkBulkLoaderStateMachine.vertexDataset.repartition(
                        sparkBulkLoaderStateMachine.vertexPartitionCount, new Column("~id"));

                // Update the vertex partition count in the state machine.
                RecoveryUtil.updateVertexRecovery(
                        sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(),
                        sparkBulkLoaderStateMachine.vertexPartitionCount);
            }
        }
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateReadEdges(sparkBulkLoaderStateMachine);
    }

    @Override
    protected BulkLoadStateStatusMap getStateMap() {
        return new BulkLoadStateStatusMap("reading vertex data", false, BULK_LOAD_STATUS_IN_PROGRESS);
    }
}
