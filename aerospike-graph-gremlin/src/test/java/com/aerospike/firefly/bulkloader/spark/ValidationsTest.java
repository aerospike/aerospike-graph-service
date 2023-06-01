package com.aerospike.firefly.bulkloader.spark;

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

    private SparkSession spark;
    public void setUp() throws Exception {
        spark = SparkSession
                .builder()
                .appName("validations test")
                .master("local[1]") // We use local mode for tests
                .getOrCreate();
    }

    public void tearDown() throws Exception {
        if (spark != null) {
            spark.stop();
        }
    }

    public void testTestDupilcateVertices() {

        final String groupColumn ="~id";
        //nertices id 1,2 are present multiple times in dataframe
        Dataset<Row> df = spark.createDataFrame(Arrays.asList(
                RowFactory.create("1", 1, "file1"),
                RowFactory.create("1", 5, "file2"),
                RowFactory.create("2", 4 , "file3"),
                RowFactory.create("2", 9 , "file3"),
                RowFactory.create("3", 1, "file1")
        ), new StructType(new StructField[]{
                new StructField(groupColumn, DataTypes.StringType, false, Metadata.empty()),
                new StructField(DatasetOperations.LINENUMBER_COLUMN, DataTypes.IntegerType, false, Metadata.empty()),
                new StructField(DatasetOperations.FILENAME_COLUMN, DataTypes.StringType, false, Metadata.empty())
        }));

        //should be false because we have two vertices with greater than one cardinality
        assertFalse(Validations.testDupilcateVertices(df,null));
    }
}