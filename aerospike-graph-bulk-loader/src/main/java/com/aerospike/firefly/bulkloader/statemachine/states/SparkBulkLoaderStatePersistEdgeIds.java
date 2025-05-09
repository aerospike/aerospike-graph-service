package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.BulkLoadStateStatusMap;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.PACKING_ID_COLUMN;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.FROM_VERTEX_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.TO_VERTEX_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyElement.ID_HEADER;
import static com.aerospike.firefly.bulkloader.util.ExceptionMessages.BAD_EDGE_COUNT_EXCEEDED;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_IN_PROGRESS;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.ALLOWED_BAD_EDGES_COUNT;
import static org.apache.spark.sql.functions.col;

public class SparkBulkLoaderStatePersistEdgeIds extends SparkBulkLoaderState {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderStatePersistEdgeIds.class);
    private static final String TO_ID_HEADER = "~to_id";
    private static final String FROM_ID_HEADER = "~from_id";

    public SparkBulkLoaderStatePersistEdgeIds(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Persist edge ids.
        // Persist Edge ID data to disk

        // Now the order of ids in the partition should be preserved.
        if (sparkBulkLoaderStateMachine.generateEdgeCaches) {
            // Find dangling edges and remove from edge dataset.
            LOGGER.info("Removing dangling edges");
            // Create one dataset with all distinct vertex IDs from both TO and FROM
            Dataset<Row> referencedVertices = sparkBulkLoaderStateMachine.edgeDataset
                    .select(col(TO_VERTEX_HEADER).alias(ID_HEADER))
                    .union(sparkBulkLoaderStateMachine.edgeDataset
                            .select(col(FROM_VERTEX_HEADER).alias(ID_HEADER)))
                    .filter(col(ID_HEADER).isNotNull())
                    .distinct();
            Dataset<Row> missingVertices = referencedVertices
                    .join(sparkBulkLoaderStateMachine.vertexDataset.select(ID_HEADER),
                            referencedVertices.col(ID_HEADER).equalTo(sparkBulkLoaderStateMachine.vertexDataset.col(ID_HEADER)),
                            "left_anti");
            sparkBulkLoaderStateMachine.edgeDataset = sparkBulkLoaderStateMachine.edgeDataset
                    // Remove edges where TO_VERTEX_HEADER is a missing vertex
                    .join(missingVertices.withColumnRenamed(ID_HEADER, TO_VERTEX_HEADER),
                            sparkBulkLoaderStateMachine.edgeDataset.col(TO_VERTEX_HEADER)
                                    .equalTo(missingVertices.col(TO_VERTEX_HEADER)),
                            "left_anti")
                    // Remove edges where FROM_VERTEX_HEADER is a missing vertex
                    .join(missingVertices.withColumnRenamed(ID_HEADER, FROM_VERTEX_HEADER),
                            sparkBulkLoaderStateMachine.edgeDataset.col(FROM_VERTEX_HEADER)
                                    .equalTo(missingVertices.col(FROM_VERTEX_HEADER)),
                            "left_anti");
            LOGGER.info("Dangling edges removed");
        }

        if (!sparkBulkLoaderStateMachine.readOnly) {
            LOGGER.info("Writing ids");
            sparkBulkLoaderStateMachine.edgeDataset = sparkBulkLoaderStateMachine.edgeOperations.writeEdgeIDsToDataframe(
                    sparkBulkLoaderStateMachine.edgeDataset,
                    sparkBulkLoaderStateMachine.edgeRecoveryDirectory,
                    sparkBulkLoaderStateMachine.fileConfig,
                    sparkBulkLoaderStateMachine.readOnly);

            // Latch recovery directory.
            LOGGER.info("Ids written");
            RecoveryUtil.writeTempEdgeDirectory(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), sparkBulkLoaderStateMachine.edgeRecoveryDirectory);
        }

        if (sparkBulkLoaderStateMachine.generateEdgeCaches) {
            // Calculate after they are written so data is fresh.
            // TODO: Remove.
            LOGGER.info("Starting edge counting");
            Instant start = Instant.now();
            sparkBulkLoaderStateMachine.edgeCountAfterRemoval = sparkBulkLoaderStateMachine.edgeDataset.count();
            LOGGER.info("Edge count after removal of dangling edges: {} took {} ms",
                    sparkBulkLoaderStateMachine.edgeCountAfterRemoval,
                    Duration.between(start, Instant.now()).toMillis());
            sparkBulkLoaderStateMachine.progressBar.setEdgeTotalCount(sparkBulkLoaderStateMachine.edgeCountAfterRemoval);

            final long allowedDetachedEdges = sparkBulkLoaderStateMachine.config.getOrDefaultInt(ALLOWED_BAD_EDGES_COUNT);
            LOGGER.info("Allowed detached edges: {}", allowedDetachedEdges);

            if ((sparkBulkLoaderStateMachine.edgeCount - sparkBulkLoaderStateMachine.edgeCountAfterRemoval) > allowedDetachedEdges) {
                throw new RuntimeException(BAD_EDGE_COUNT_EXCEEDED);
            }
        }

        sparkBulkLoaderStateMachine.edgePartitionCount = sparkBulkLoaderStateMachine.edgeDataset.rdd().getPartitions().length;
        LOGGER.info("EdgeId dataset has {} partitions", sparkBulkLoaderStateMachine.edgePartitionCount);

        if (!sparkBulkLoaderStateMachine.readOnly) {
            // Update edge recovery info.
            RecoveryUtil.updateEdgeRecovery(
                    sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(),
                    sparkBulkLoaderStateMachine.edgePartitionCount);
            sparkBulkLoaderStateMachine.edgeDataset = sparkBulkLoaderStateMachine.edgeDataset.repartition(
                    sparkBulkLoaderStateMachine.edgePartitionCount, new Column(PACKING_ID_COLUMN));
            sparkBulkLoaderStateMachine.progressBar.setEdgePartitionCount(sparkBulkLoaderStateMachine.edgePartitionCount);
            sparkBulkLoaderStateMachine.edgeDataset.sortWithinPartitions(new Column(PACKING_ID_COLUMN));
        }

        sparkBulkLoaderStateMachine.progressBar.setEdgeIdWriteComplete();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateDetectSupernodes(sparkBulkLoaderStateMachine);
    }

    @Override
    protected BulkLoadStateStatusMap getStateMap() {
        return new BulkLoadStateStatusMap("generating edge IDs", false, BULK_LOAD_STATUS_IN_PROGRESS);
    }
}
