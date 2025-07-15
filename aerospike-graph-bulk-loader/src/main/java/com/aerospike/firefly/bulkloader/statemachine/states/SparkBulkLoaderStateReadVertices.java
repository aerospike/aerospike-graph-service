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
        // Latch info for incremental load.
        RecoveryUtil.writeIsIncrementalLoad(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), sparkBulkLoaderStateMachine.incrementalLoad);
        if (sparkBulkLoaderStateMachine.incrementalLoad) {
            // Get summary and set initial values for incremental load.
            FireflyGraphSummaryUpdater.FireflyElementMetadata summary =
                    sparkBulkLoaderStateMachine.initializerGraph.fireflySummaryUpdater.getFireflyStatistics();
            RecoveryUtil.writeIncrementalLoadVertexStartCount(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), summary.totalVertexCount());
            RecoveryUtil.writeIncrementalLoadEdgeStartCount(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), summary.totalEdgeCount());
        }

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

        // This stuff is done in the edge cache generation state, but we need to do it here in incremental mode.
        // Won't be set in later stage if incremental.
        // isEdgeCacheWrittenWithVertex is set to true for non-incremental & non-readonly.
        if (!sparkBulkLoaderStateMachine.isEdgeCacheWrittenWithVertex) {
            // Need this regardless of read-only or not.
            sparkBulkLoaderStateMachine.vertexPartitionCount = sparkBulkLoaderStateMachine.vertexDataset.rdd().partitions().length;

            // Assign partition count.
            if (!sparkBulkLoaderStateMachine.readOnly) {
                // Partition by id for consistency and recoverability if not readonly.
                sparkBulkLoaderStateMachine.vertexDataset = sparkBulkLoaderStateMachine.vertexDataset.repartition(
                        sparkBulkLoaderStateMachine.vertexPartitionCount, new Column("~id"));

                RecoveryUtil.updateVertexRecovery(
                        sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(),
                        sparkBulkLoaderStateMachine.vertexPartitionCount);
            }

            // Set partition count.
            sparkBulkLoaderStateMachine.progressBar.setVertexPartitionCount(sparkBulkLoaderStateMachine.vertexPartitionCount);
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
