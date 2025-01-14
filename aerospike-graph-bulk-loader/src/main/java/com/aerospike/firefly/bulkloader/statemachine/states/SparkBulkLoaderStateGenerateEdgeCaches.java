package com.aerospike.firefly.bulkloader.statemachine.states;

import com.aerospike.firefly.bulkloader.statemachine.machine.SparkBulkLoaderStateMachine;
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
import java.sql.Struct;

import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.FROM_VERTEX_CACHE_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.FROM_VERTEX_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.TO_VERTEX_CACHE_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge.TO_VERTEX_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyElement.ID_HEADER;
import static com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyElement.LABEL_HEADER;
import static com.aerospike.firefly.process.call.bulkload.utils.BulkLoaderConfigHelper.TEMP_DIRECTORY_KEY;
import static org.apache.spark.sql.functions.*;

public class SparkBulkLoaderStateGenerateEdgeCaches extends SparkBulkLoaderState {
    private static final Logger LOGGER = LoggerFactory.getLogger(SparkBulkLoaderStateGenerateEdgeCaches.class);

    public SparkBulkLoaderStateGenerateEdgeCaches(final SparkBulkLoaderStateMachine sparkBulkLoaderStateMachine) {
        super(sparkBulkLoaderStateMachine);
    }

    @Override
    public void executeState() {
        final String vertexMergedDataset = RecoveryUtil.getEdgeRecoveryDirectory(
                sparkBulkLoaderStateMachine.config.getOrDefault(TEMP_DIRECTORY_KEY),
                sparkBulkLoaderStateMachine.fileSystem.equals(SparkBulkLoaderStateMachine.LOCAL)
                        ? File.separator : "/");

        // Precompute edge caches from edge dataset.
        //Dataset<Row> fromEdgeDataset = sparkBulkLoaderStateMachine.edgeDataset.
        //        groupBy(new Column(FROM_VERTEX_HEADER)).
        //        agg(functions.collect_list(
        //                functions.array(new Column(ID_HEADER), new Column(TO_VERTEX_HEADER))).alias(FROM_VERTEX_CACHE_HEADER))
        //        .withColumn(FROM_VERTEX_CACHE_HEADER,
        //                functions.when(new Column(FROM_VERTEX_CACHE_HEADER).isNotNull(),
        //                        functions.to_json(new Column(FROM_VERTEX_CACHE_HEADER))).otherwise(null));

        Dataset<Row> fromEdgeDataset = sparkBulkLoaderStateMachine.edgeDataset.
                groupBy(col(FROM_VERTEX_HEADER), col(LABEL_HEADER)).
                agg(collect_list(array(col(TO_VERTEX_HEADER), col(ID_HEADER))).alias("tmp")).
                groupBy(col(FROM_VERTEX_HEADER)).
                agg(map_from_entries(collect_list(struct(col(LABEL_HEADER), col("tmp")))).
                        alias(FROM_VERTEX_CACHE_HEADER)).
                withColumn(FROM_VERTEX_CACHE_HEADER,
                        functions.when(new Column(FROM_VERTEX_CACHE_HEADER).isNotNull(),
                                functions.to_json(new Column(FROM_VERTEX_CACHE_HEADER))).otherwise(null));

        //fromEdgeDataset.show(10, false);
        //Dataset<Row> toEdgeDataset = sparkBulkLoaderStateMachine.edgeDataset.
        //        groupBy(new Column(TO_VERTEX_HEADER)).
        //        agg(functions.collect_list(
        //                functions.array(new Column(ID_HEADER), new Column(FROM_VERTEX_HEADER))).alias(TO_VERTEX_CACHE_HEADER))
        //        .withColumn(TO_VERTEX_CACHE_HEADER,
        //                functions.when(new Column(TO_VERTEX_CACHE_HEADER).isNotNull(),
        //                        functions.to_json(new Column(TO_VERTEX_CACHE_HEADER))).otherwise(null));

        Dataset<Row> toEdgeDataset = sparkBulkLoaderStateMachine.edgeDataset.
                groupBy(col(TO_VERTEX_HEADER), col(LABEL_HEADER)).
                agg(collect_list(array(col(FROM_VERTEX_HEADER), col(ID_HEADER))).alias("tmp")).
                groupBy(col(TO_VERTEX_HEADER)).
                agg(map_from_entries(collect_list(struct(col(LABEL_HEADER), col("tmp")))).
                        alias(TO_VERTEX_CACHE_HEADER)).
                withColumn(TO_VERTEX_CACHE_HEADER,
                        functions.when(new Column(TO_VERTEX_CACHE_HEADER).isNotNull(),
                                functions.to_json(new Column(TO_VERTEX_CACHE_HEADER))).otherwise(null));


        //toEdgeDataset.show(10, false);
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

        // TODO:
        // sparkBulkLoaderStateMachine.edgeDataset.drop(TO_VERTEX_HEADER); / FROM.
        //
        sparkBulkLoaderStateMachine.vertexDataset.write().option("header", true).
                mode(SaveMode.Overwrite).option("compression", "bzip2").csv(vertexMergedDataset);
    }

    @Override
    public SparkBulkLoaderState transitionState() {
        return new SparkBulkLoaderStateWriteVertices(sparkBulkLoaderStateMachine);
    }

}
