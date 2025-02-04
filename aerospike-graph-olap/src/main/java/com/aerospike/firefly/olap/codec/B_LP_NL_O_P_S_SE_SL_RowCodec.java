package com.aerospike.firefly.olap.codec;

import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_NL_O_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.List;
import java.util.Set;

public class B_LP_NL_O_P_S_SE_SL_RowCodec extends RowCodec {
    private static final B_LP_NL_O_P_S_SE_SL_RowCodec INSTANCE = new B_LP_NL_O_P_S_SE_SL_RowCodec();

    public static B_LP_NL_O_P_S_SE_SL_RowCodec getInstance() {
        return INSTANCE;
    }

    private B_LP_NL_O_P_S_SE_SL_RowCodec() {
        super(List.of(CodecRequirements.BASE, CodecRequirements.BULK, CodecRequirements.PATH, CodecRequirements.SINGLE_LOOP, CodecRequirements.NESTED_LOOP));
    }

    /////////////////////////////////////////////////////////////////////
    // Requirements
    /////////////////////////////////////////////////////////////////////

    @Override
    Set<TraverserRequirement> getRequirements() {
        return B_LP_NL_O_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements();
    }
}
