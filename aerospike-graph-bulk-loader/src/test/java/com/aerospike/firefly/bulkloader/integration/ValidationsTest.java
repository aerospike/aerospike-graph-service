package com.aerospike.firefly.bulkloader.integration;

import com.aerospike.firefly.bulkloader.spark.DatasetOperations;
import com.aerospike.firefly.bulkloader.spark.Validations;
import junit.framework.TestCase;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Arrays;

public class ValidationsTest extends TestCase {
    private static SparkSession SPARK;

    @Override
    public void setUp() {
        SPARK = SparkSession
                .builder()
                .appName("validations test")
                .master("local[1]") // We use local mode for tests
                .getOrCreate();
    }

    @Override
    public void tearDown() throws Exception {
        if (SPARK != null) {
            SPARK.stop();
        }
    }

    public void testTestDuplicateVertices() {
        final String groupColumn ="~id";
        final String lineColumn ="~line";
        // Vertices with ~id 1 and 2 are present multiple times in dataframe
        Dataset<Row> df = SPARK.createDataFrame(Arrays.asList(
                RowFactory.create("1", 1L, "file1"),
                RowFactory.create("1", 5L, "file2"),
                RowFactory.create("2", 4L, "file3"),
                RowFactory.create("2", 9L, "file3"),
                RowFactory.create("3", 1L, "file1")
        ), new StructType(new StructField[]{
                new StructField(groupColumn, DataTypes.StringType, false, Metadata.empty()),
                new StructField(lineColumn, DataTypes.LongType, false, Metadata.empty()),
                new StructField(DatasetOperations.FILENAME_COLUMN, DataTypes.StringType, false, Metadata.empty())
        }));

        // Should be false because we have two vertices with cardinality greater than one
        assertFalse(Validations.validateNoDuplicateVertexIds(df));
    }
}
