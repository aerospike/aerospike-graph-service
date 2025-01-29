package com.aerospike.firefly.olap.codec.decoder;

import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;

import java.util.Set;

public class LP_O_OB_S_SE_SL_RowDecoder implements RowDecoder {
    private static final LP_O_OB_S_SE_SL_RowDecoder INSTANCE = new LP_O_OB_S_SE_SL_RowDecoder();

    public static LP_O_OB_S_SE_SL_RowDecoder getInstance() {
        return INSTANCE;
    }

    private LP_O_OB_S_SE_SL_RowDecoder() {
    }

    public Traverser decode(final Row row, final TraverserGenerator tg, final TraversalMatrix tm) {
        return null;
    }

    public Set<TraverserRequirement> getRequirements() {
        return LP_O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements();
    }
}
