package com.aerospike.firefly.olap.process.traversal.step;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public interface SparkOperation {
    Dataset<Row> operate(final Dataset<Row> df);
    boolean canContinueDistributed();
    int sparkColumnsCount();
}
