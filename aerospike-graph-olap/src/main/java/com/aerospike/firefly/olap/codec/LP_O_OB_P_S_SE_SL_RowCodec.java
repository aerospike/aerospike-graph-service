package com.aerospike.firefly.olap.codec;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;
import java.util.Set;

public class LP_O_OB_P_S_SE_SL_RowCodec extends RowCodec {
    private static final LP_O_OB_P_S_SE_SL_RowCodec INSTANCE = new LP_O_OB_P_S_SE_SL_RowCodec();

    public static LP_O_OB_P_S_SE_SL_RowCodec getInstance() {
        return INSTANCE;
    }

    private LP_O_OB_P_S_SE_SL_RowCodec() {
        super(List.of());
    }

    /////////////////////////////////////////////////////////////////////
    // Requirements
    /////////////////////////////////////////////////////////////////////

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return LP_O_OB_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements();
    }
}
