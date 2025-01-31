package com.aerospike.firefly.olap.codec;

import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.List;
import java.util.Set;

public class RowCodecFactory {
    private static final RowCodecFactory INSTANCE = new RowCodecFactory();

    private RowCodecFactory() {
    }

    public static RowCodecFactory getInstance() {
        return INSTANCE;
    }


    public RowCodec getCodec(final Set<TraverserRequirement> requirements) {
        if (requirements.contains(TraverserRequirement.ONE_BULK)) {
            if (O_OB_S_SE_SL_RowCodec.getInstance().getRequirements().containsAll(requirements))
                return O_OB_S_SE_SL_RowCodec.getInstance();

            if (NL_O_OB_S_SE_SL_RowCodec.getInstance().getRequirements().containsAll(requirements))
                return NL_O_OB_S_SE_SL_RowCodec.getInstance();

            if (LP_O_OB_S_SE_SL_RowCodec.getInstance().getRequirements().containsAll(requirements))
                return LP_O_OB_S_SE_SL_RowCodec.getInstance();

            if (LP_NL_O_OB_S_SE_SL_RowCodec.getInstance().getRequirements().containsAll(requirements))
                return LP_NL_O_OB_S_SE_SL_RowCodec.getInstance();

            if (LP_O_OB_P_S_SE_SL_RowCodec.getInstance().getRequirements().containsAll(requirements))
                return LP_O_OB_P_S_SE_SL_RowCodec.getInstance();

            if (LP_NL_O_OB_P_S_SE_SL_RowCodec.getInstance().getRequirements().containsAll(requirements))
                return LP_NL_O_OB_P_S_SE_SL_RowCodec.getInstance();
        } else {
            if (B_O_RowCodec.getInstance().getRequirements().containsAll(requirements))
                return B_O_RowCodec.getInstance();

            if (B_O_S_SE_SL_RowCodec.getInstance().getRequirements().containsAll(requirements))
                return B_O_S_SE_SL_RowCodec.getInstance();

            if (B_NL_O_S_SE_SL_RowCodec.getInstance().getRequirements().containsAll(requirements))
                return B_NL_O_S_SE_SL_RowCodec.getInstance();

            if (B_LP_O_S_SE_SL_RowCodec.getInstance().getRequirements().containsAll(requirements))
                return B_LP_O_S_SE_SL_RowCodec.getInstance();

            if (B_LP_NL_O_S_SE_SL_RowCodec.getInstance().getRequirements().containsAll(requirements))
                return B_LP_NL_O_S_SE_SL_RowCodec.getInstance();

            if (B_LP_O_P_S_SE_SL_RowCodec.getInstance().getRequirements().containsAll(requirements))
                return B_LP_O_P_S_SE_SL_RowCodec.getInstance();

            if (B_LP_NL_O_P_S_SE_SL_RowCodec.getInstance().getRequirements().containsAll(requirements))
                return B_LP_NL_O_P_S_SE_SL_RowCodec.getInstance();
        }

        throw new IllegalStateException("The provided traverser generator factory does not support the requirements of the traversal: " + this.getClass().getCanonicalName() + requirements);
    }
}
