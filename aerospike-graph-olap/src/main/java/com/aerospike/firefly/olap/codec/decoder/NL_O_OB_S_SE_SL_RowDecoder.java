package com.aerospike.firefly.olap.codec.decoder;

import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.NL_O_OB_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;

import java.util.Set;

public class NL_O_OB_S_SE_SL_RowDecoder implements RowDecoder {
    private static final NL_O_OB_S_SE_SL_RowDecoder INSTANCE = new NL_O_OB_S_SE_SL_RowDecoder();

    public static NL_O_OB_S_SE_SL_RowDecoder getInstance() {
        return INSTANCE;
    }

    private NL_O_OB_S_SE_SL_RowDecoder() {
    }

    public Traverser decode(final Row row, final TraverserGenerator tg, final TraversalMatrix tm) {
        return null;
    }

    public Set<TraverserRequirement> getRequirements() {
        return NL_O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements();
    }
}
