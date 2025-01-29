package com.aerospike.firefly.olap.codec.decoder;

import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_NL_O_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;

import java.util.Set;

public class B_NL_O_S_SE_SL_RowDecoder implements RowDecoder {
    private static final B_NL_O_S_SE_SL_RowDecoder INSTANCE = new B_NL_O_S_SE_SL_RowDecoder();

    public static B_NL_O_S_SE_SL_RowDecoder getInstance() {
        return INSTANCE;
    }

    private B_NL_O_S_SE_SL_RowDecoder() {
    }

    public Traverser decode(final Row row, final TraverserGenerator tg, final TraversalMatrix tm) {
        return null;
    }

    public Set<TraverserRequirement> getRequirements() {
        return B_NL_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements();
    }
}
