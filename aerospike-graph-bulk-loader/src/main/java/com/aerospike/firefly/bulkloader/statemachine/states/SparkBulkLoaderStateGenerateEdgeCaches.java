package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.BulkLoadStateStatusMap;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.time.Instant;

import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.EDGE_ID_COLUMN;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.FROM_VERTEX_CACHE_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.FROM_VERTEX_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.TO_VERTEX_CACHE_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.TO_VERTEX_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyElement.ID_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyElement.LABEL_HEADER;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoadStatusTokens.BULK_LOAD_STATUS_IN_PROGRESS;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.TEMP_DIRECTORY_KEY;
import static org.apache.spark.sql.functions.*;

public class SparkBulkLoaderStateGenerateEdgeCaches extends SparkBulkLoaderState {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderStateGenerateEdgeCaches.class);

    public SparkBulkLoaderStateGenerateEdgeCaches(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Since we can't persist the edgeIds, spark re-generates them over and over and it screws up the ids.
        if (!sparkBulkLoaderStateMachine.isEdgeCacheWrittenWithVertex) {
            return;
        }

        // Update spark to indicate we are generating edge caches.
        final Instant edgeCacheGenerationTime = Instant.now();
        final String taskName = "Edge Cache Generation";
        sparkBulkLoaderStateMachine.spark.sparkContext().
                setJobGroup(taskName, "Edge cache generation task.", true);

        // Create the edge cache data.
        final Dataset<Row> fromEdgeDataset = sparkBulkLoaderStateMachine.edgeDataset.
                groupBy(col(FROM_VERTEX_HEADER), col(LABEL_HEADER)).
                agg(collect_list(array(col(TO_VERTEX_HEADER), col(EDGE_ID_COLUMN))).alias("tmp")).
                filter(col(LABEL_HEADER).isNotNull()).
                filter(not(col(FROM_VERTEX_HEADER).isin(sparkBulkLoaderStateMachine.supernodes.toArray()))).
                groupBy(col(FROM_VERTEX_HEADER)).
                agg(map_from_entries(collect_list(struct(col(LABEL_HEADER), col("tmp")))).
                        alias(FROM_VERTEX_CACHE_HEADER)).
                withColumn(FROM_VERTEX_CACHE_HEADER,
                        functions.when(new Column(FROM_VERTEX_CACHE_HEADER).isNotNull(),
                                functions.to_json(new Column(FROM_VERTEX_CACHE_HEADER))).otherwise(null));
        final Dataset<Row> toEdgeDataset = sparkBulkLoaderStateMachine.edgeDataset.
                groupBy(col(TO_VERTEX_HEADER), col(LABEL_HEADER)).
                agg(collect_list(array(col(FROM_VERTEX_HEADER), col(EDGE_ID_COLUMN))).alias("tmp")).
                filter(col(LABEL_HEADER).isNotNull()).
                filter(not(col(TO_VERTEX_HEADER).isin(sparkBulkLoaderStateMachine.supernodes.toArray()))).
                groupBy(col(TO_VERTEX_HEADER)).
                agg(map_from_entries(collect_list(struct(col(LABEL_HEADER), col("tmp")))).
                        alias(TO_VERTEX_CACHE_HEADER)).
                withColumn(TO_VERTEX_CACHE_HEADER,
                        functions.when(new Column(TO_VERTEX_CACHE_HEADER).isNotNull(),
                                functions.to_json(new Column(TO_VERTEX_CACHE_HEADER))).otherwise(null));

        // This is a testing config, used to force failure in specific spots to allow us to test the recovery modes.
        final String failOnEdgeCaches = sparkBulkLoaderStateMachine.config.getOrDefault(BulkLoaderConfigHelper.RECOVERY_FAILURE);
        if (RecoveryUtil.RecoveryState.GENERATE_EDGE_CACHES.name().equals(failOnEdgeCaches)) {
            throw new RuntimeException("Testing recovery failure, please contact support.");
        }

        // Merge in the edge cache data into the vertex dataset.
        sparkBulkLoaderStateMachine.vertexDataset = sparkBulkLoaderStateMachine.vertexDataset.join(
                fromEdgeDataset,
                sparkBulkLoaderStateMachine.vertexDataset.col(ID_HEADER).equalTo(
                        fromEdgeDataset.col(FROM_VERTEX_HEADER)),
                "left").drop(FROM_VERTEX_HEADER);
        sparkBulkLoaderStateMachine.vertexDataset = sparkBulkLoaderStateMachine.vertexDataset.join(
                toEdgeDataset,
                sparkBulkLoaderStateMachine.vertexDataset.col(ID_HEADER).equalTo(
                        toEdgeDataset.col(TO_VERTEX_HEADER)),
                "left").drop(TO_VERTEX_HEADER);

        // Get the directory to save the merged dataset & save dataset.
        final String vertexMergedDataset = RecoveryUtil.getVertexRecoveryDirectory(
                sparkBulkLoaderStateMachine.config.getOrDefault(TEMP_DIRECTORY_KEY),
                sparkBulkLoaderStateMachine.fileSystem.equals(SparkBulkLoaderStateMachine.LOCAL)
                        ? File.separator : "/");
        sparkBulkLoaderStateMachine.vertexDataset.write().option("header", true).
                mode(SaveMode.Overwrite).option("compression", "snappy").parquet(vertexMergedDataset);

        // Latch and log vertex dataset partition count.
        sparkBulkLoaderStateMachine.vertexPartitionCount = sparkBulkLoaderStateMachine.vertexDataset.rdd().partitions().length;
        LOGGER.info("Vertex dataset has {} partitions", sparkBulkLoaderStateMachine.vertexPartitionCount);

        // Store vertex partitioning information for recovery.
        sparkBulkLoaderStateMachine.progressBar.setVertexPartitionCount(sparkBulkLoaderStateMachine.vertexPartitionCount);
        RecoveryUtil.writeTempVertexDirectory(sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), vertexMergedDataset);
        RecoveryUtil.updateVertexRecovery(
                sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(),
                sparkBulkLoaderStateMachine.vertexPartitionCount);

        // Partition by id for consistency.
        sparkBulkLoaderStateMachine.vertexDataset = sparkBulkLoaderStateMachine.vertexDataset.repartition(
                sparkBulkLoaderStateMachine.vertexPartitionCount, new Column("~id"));

        // Stop job.
        sparkBulkLoaderStateMachine.spark.sparkContext().cancelJobGroup(taskName);

        // Log the time taken for edge cache generation.
        LOGGER.info("Execution time in seconds for Edge cache generation task: " +
                Duration.between(edgeCacheGenerationTime, Instant.now()).getSeconds());

        // Update the progress bar.
        sparkBulkLoaderStateMachine.progressBar.setGenerateEdgeCachesComplete();
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateWriteVertices(sparkBulkLoaderStateMachine);
    }

    @Override
    protected BulkLoadStateStatusMap getStateMap() {
        return new BulkLoadStateStatusMap("generating edge caches", false, BULK_LOAD_STATUS_IN_PROGRESS);
    }
}
