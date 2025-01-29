package com.aerospike.firefly.olap.codec.schema;

import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.Set;

public class RowSchemaFactory {
    private static final RowSchemaFactory INSTANCE = new RowSchemaFactory();

    private RowSchemaFactory() {
    }

    public static RowSchemaFactory getInstance() {
        return INSTANCE;
    }

    public RowSchema getSchema(final Set<TraverserRequirement> requirements) {
        // TODO.
        return B_O_RowSchema.getInstance();
    }
}
