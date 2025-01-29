package com.aerospike.firefly.olap.codec.schema;

import org.apache.spark.sql.types.StructType;

public class B_O_RowSchema extends RowSchema {
    private static final B_O_RowSchema INSTANCE = new B_O_RowSchema();

    private B_O_RowSchema() {
    }

    public static B_O_RowSchema getInstance() {
        return INSTANCE;
    }

    @Override
    public StructType getSchema() {
        return getBaseSchema();
    }
}
