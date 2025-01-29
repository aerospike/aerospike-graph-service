package com.aerospike.firefly.olap.codec.schema;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

// TODO: Need to finish these.
public abstract class RowSchema {

    public static final String TRAVERSER_TYPE_COL = "~traverser_type";
    public static final String ID_COL = "~id";
    public static final String ID_TYPEHINT_COL = "~id_typehint";
    public static final String LABEL_COL = "~label";
    public static final String PROPERTIES_COL = "~properties";
    public static final String IN_COL = "~in";
    public static final String OUT_COL = "~out";
    public static final String HALTED_COL = "~halted";
    public static final String REF_COL = "~ref";
    public static final String STEP_COL = "~step";
    public static final String BULK_COL = "~bulk";

    protected StructType getBaseSchema() {
        return new StructType()
                .add(TRAVERSER_TYPE_COL, DataTypes.IntegerType, false)
                .add(ID_COL, DataTypes.StringType, false)
                .add(ID_TYPEHINT_COL, DataTypes.IntegerType, false)
                .add(LABEL_COL, DataTypes.StringType, false)
                .add(HALTED_COL, DataTypes.BooleanType, false)
                .add(STEP_COL, DataTypes.StringType, false)
                .add(BULK_COL, DataTypes.LongType, false);
    }

    abstract public StructType getSchema();
}
