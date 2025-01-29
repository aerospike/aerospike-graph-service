package com.aerospike.firefly.olap.codec.encoder;

import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_NL_O_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_NL_O_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.EnumSet;
import java.util.Set;

public class B_LP_NL_O_S_SE_SL_RowEncoder implements RowEncoder {
    private static final B_LP_NL_O_S_SE_SL_RowEncoder INSTANCE = new B_LP_NL_O_S_SE_SL_RowEncoder();

    public static B_LP_NL_O_S_SE_SL_RowEncoder getInstance() {
        return INSTANCE;
    }

    private B_LP_NL_O_S_SE_SL_RowEncoder() {
    }

    public Row encode(final Traverser traverser) {
        return null;
    }

    public Row encode(final Vertex vertex, final String step) {
        return null;
    }

    public Row encode(final Edge edge, final String step) {
        return null;
    }

    public Set<TraverserRequirement> getRequirements() {
        return B_LP_NL_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements();
    }
}
