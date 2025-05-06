package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
import com.aerospike.firefly.bulkloader.util.BulkLoadStateStatusMap;
import com.aerospike.firefly.bulkloader.util.RecoveryUtil;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Set;

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

    public SparkBulkLoaderStateGenerateEdgeCaches(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        // Since we can't persist the edgeIds, spark re-generates them over and over and it screws up the ids.
        if (sparkBulkLoaderStateMachine.readOnly) {
            return;
        }
        RecoveryUtil.updateState(
                sparkBulkLoaderStateMachine.initializerGraph.getBaseGraph(), RecoveryUtil.RecoveryState.DETECT_SUPERNODES);
        sparkBulkLoaderStateMachine.progressBar.setPreflightCheckComplete();

        final String vertexMergedDataset = RecoveryUtil.getEdgeRecoveryDirectory(
                sparkBulkLoaderStateMachine.config.getOrDefault(TEMP_DIRECTORY_KEY),
                sparkBulkLoaderStateMachine.fileSystem.equals(SparkBulkLoaderStateMachine.LOCAL)
                        ? File.separator : "/");

        Dataset<Row> fromEdgeDataset = sparkBulkLoaderStateMachine.edgeDataset.
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
        Dataset<Row> toEdgeDataset = sparkBulkLoaderStateMachine.edgeDataset.
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

        sparkBulkLoaderStateMachine.vertexDataset.write().option("header", true).
                mode(SaveMode.Overwrite).option("compression", "bzip2").csv(vertexMergedDataset);

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
