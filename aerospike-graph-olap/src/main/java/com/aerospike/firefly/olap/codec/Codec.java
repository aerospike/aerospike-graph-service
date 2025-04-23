package com.aerospike.firefly.olap.codec;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;

public interface Codec {
    Row encode(Traverser traverser);

    Traverser decode(Row row);

    StructType getSchema();

    TraverserGenerator getTraverserGenerator();

    default boolean isBulkingSupported() {
        return false;
    }

    default int getBulkedOrdinal() {
        return 1;
    }
}
