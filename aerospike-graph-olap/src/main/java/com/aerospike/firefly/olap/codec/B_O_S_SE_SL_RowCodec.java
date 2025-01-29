package com.aerospike.firefly.olap.codec;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

public class B_O_S_SE_SL_RowCodec extends RowCodec {
    private static final B_O_S_SE_SL_RowCodec INSTANCE = new B_O_S_SE_SL_RowCodec();

    public static B_O_S_SE_SL_RowCodec getInstance() {
        return INSTANCE;
    }

    /////////////////////////////////////////////////////////////////////
    // Schema
    /////////////////////////////////////////////////////////////////////

    @Override
    StructType getSchema() {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /////////////////////////////////////////////////////////////////////
    // Decode
    /////////////////////////////////////////////////////////////////////
    @Override
    Traverser decode(final Row row, final TraverserGenerator tg, final TraversalMatrix tm) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /////////////////////////////////////////////////////////////////////
    // Encode
    /////////////////////////////////////////////////////////////////////

    @Override
    Row encode(final Traverser traverser) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    Row encode(final Vertex vertex, final String step) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    @Override
    Row encode(final Edge edge, final String step) {
        throw new UnsupportedOperationException("Not implemented yet");
    }

    /////////////////////////////////////////////////////////////////////
    // Requirements
    /////////////////////////////////////////////////////////////////////

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return B_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements();
    }
}
