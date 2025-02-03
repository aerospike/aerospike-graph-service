package com.aerospike.firefly.olap.codec;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_NL_O_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;
import java.util.Set;

public class B_LP_NL_O_P_S_SE_SL_RowCodec extends RowCodec {
    private static final B_LP_NL_O_P_S_SE_SL_RowCodec INSTANCE = new B_LP_NL_O_P_S_SE_SL_RowCodec();

    public static B_LP_NL_O_P_S_SE_SL_RowCodec getInstance() {
        return INSTANCE;
    }

    private B_LP_NL_O_P_S_SE_SL_RowCodec() {
        super(List.of());
    }

    /////////////////////////////////////////////////////////////////////
    // Requirements
    /////////////////////////////////////////////////////////////////////

    @Override
    Set<TraverserRequirement> getRequirements() {
        return B_LP_NL_O_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements();
    }
}
