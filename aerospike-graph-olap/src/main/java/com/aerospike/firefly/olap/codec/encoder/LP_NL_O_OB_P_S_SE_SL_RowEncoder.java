package com.aerospike.firefly.olap.codec.encoder;

import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_NL_O_OB_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.EnumSet;
import java.util.Set;

public class LP_NL_O_OB_P_S_SE_SL_RowEncoder implements RowEncoder{
    private static final LP_NL_O_OB_P_S_SE_SL_RowEncoder INSTANCE = new LP_NL_O_OB_P_S_SE_SL_RowEncoder();

    public static LP_NL_O_OB_P_S_SE_SL_RowEncoder getInstance() {
        return INSTANCE;
    }

    private LP_NL_O_OB_P_S_SE_SL_RowEncoder() {
    }

    @Override
    public Row encode(final Traverser traverser) {
        return null;
    }

    @Override
    public Row encode(final Vertex vertex, final String step) {
        return null;
    }

    @Override
    public Row encode(final Edge edge, final String step) {
        return null;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return LP_NL_O_OB_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements();
    }
}
