package com.aerospike.firefly.bulkloader.spark;

import com.aerospike.firefly.bulkloader.exception.FireflyBulkLoaderException;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyEdge;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyElement;
import com.aerospike.firefly.bulkloader.spark.structure.SparkFireflyVertex;
import com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.COLUMNSET_TO_REMOVE;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.DIRECTORY_COLUMN;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.FILENAME_COLUMN;
import static com.aerospike.firefly.bulkloader.spark.DatasetOperations.LINENUMBER_COLUMN;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.KEEP_PROVIDED_EDGE_ID_AS_PROPERTY;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.NULL_VALUE;
import static com.aerospike.firefly.bulkloader.util.BulkLoaderConfigHelper.PROVIDED_EDGE_ID_PROPERTY_NAME;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_set;
import static org.apache.spark.sql.functions.concat_ws;
import static org.apache.spark.sql.functions.count;

public class Validations {
    private static final Logger LOGGER = LoggerFactory.getLogger(Validations.class);

    private static boolean dryRunEdgeCreation(final Dataset<Row> edgeDataset, final BulkLoaderConfigHelper config) {
        final List<Integer> failures = edgeDataset.mapPartitions((MapPartitionsFunction<Row, Integer>) rowIterator -> {
            final boolean keepProvidedId =
                    Boolean.parseBoolean(config.getOrDefault(KEEP_PROVIDED_EDGE_ID_AS_PROPERTY));
            final String providedIdPropertyName = config.getOrDefault(PROVIDED_EDGE_ID_PROPERTY_NAME);
            final String nullValue = config.getOrDefault(NULL_VALUE);
            final AtomicInteger failureCount = new AtomicInteger(0);
            while (rowIterator.hasNext()) {
                final GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next();
                final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(metadataRow, COLUMNSET_TO_REMOVE);
                try {
                    SparkFireflyEdge.createEdge(fireflyRow, keepProvidedId, providedIdPropertyName, nullValue, null, true);
                } catch (FireflyBulkLoaderException e) {
                    LOGGER.error("Format validation of CSV data failed on line '{}' of file {}.",
                            metadataRow.get(metadataRow.fieldIndex(LINENUMBER_COLUMN)), metadataRow.get(metadataRow.fieldIndex(DatasetOperations.DIRECTORY_COLUMN)));
                    failureCount.incrementAndGet();
                }
            }
            return Collections.singletonList(failureCount.get()).iterator();
        }, Encoders.INT()).collectAsList();
        for (final int failure : failures) {
            if (failure != 0) {
                return false;
            }
        }
        return true;

    }
    public static boolean dryRunEdgeRows(final Dataset<Row> edgeDataset, final BulkLoaderConfigHelper config) {
        return  dryRunEdgeCreation(edgeDataset,config);
    }


    public static boolean dryRunVertices(final Dataset<Row> vertices, final BulkLoaderConfigHelper config) {
        boolean step1 = dryRunVertexCreation(vertices,config);
        boolean step2 = testDupilcateVertices(vertices, config);
        return  step1 && step2;
    }

    private static boolean dryRunVertexCreation(final Dataset<Row> vertices, final BulkLoaderConfigHelper config) {
        final String nullValue = config.getOrDefault(NULL_VALUE);
        final List<Integer> failures = vertices.mapPartitions((MapPartitionsFunction<Row, Integer>) rowIterator -> {
            final AtomicInteger failureCount = new AtomicInteger(0);
            while (rowIterator.hasNext()) {
                final GenericRowWithSchema metadataRow = (GenericRowWithSchema) rowIterator.next();
                final GenericRowWithSchema fireflyRow = DatasetOperations.removeColumns(metadataRow, COLUMNSET_TO_REMOVE);
                try {
                    SparkFireflyVertex.createVertex(fireflyRow, nullValue);
                } catch (FireflyBulkLoaderException e) {
                    LOGGER.error("Format validation of CSV data failed on line '{}' of file {}.",
                            metadataRow.get(metadataRow.fieldIndex(LINENUMBER_COLUMN)), metadataRow.get(metadataRow.fieldIndex(DIRECTORY_COLUMN)));
                    failureCount.incrementAndGet();
                }
            }
            return Collections.singletonList(failureCount.get()).iterator();
        }, Encoders.INT()).collectAsList();
        for (final int failure : failures) {
            if (failure != 0) {
                return false;
            }
        }
        return true;
    }

    public static boolean testDupilcateVertices(final Dataset<Row> vertices, final BulkLoaderConfigHelper config) {
        final String fileAndLineColumn = "~lineandFile";
        final String countColumn = "~count";
        final String idColumn = SparkFireflyElement.ID_HEADER;
        Dataset<Row> dsWithCount = vertices
                .withColumn(fileAndLineColumn,
                        concat_ws(":", col(FILENAME_COLUMN), col(LINENUMBER_COLUMN).cast("string"))) //create a new column with filename and line number to hint where error might happen
                .select(idColumn, fileAndLineColumn)
                .groupBy(idColumn).agg(collect_set(fileAndLineColumn).alias(fileAndLineColumn), count(idColumn).alias(countColumn));
        Dataset<Row> duplicateID = dsWithCount.filter(col(countColumn).gt(1));

        if(!duplicateID.isEmpty()){
            final String[] columns = duplicateID.columns();
            final int idIdx = ArrayUtils.indexOf(columns,idColumn);
            final int fileAndLineColumnIdx = ArrayUtils.indexOf(columns,fileAndLineColumn);
            final int countColumnIdx = ArrayUtils.indexOf(columns,countColumn);
            duplicateID.foreach( row -> {
                LOGGER.error("Vertex id: {}, found total {} occurrences in files {}", row.get(idIdx), row.get(countColumnIdx), row.getList(fileAndLineColumnIdx));
            });
            return false;
        }else {
            return true;
        }
    }
}
