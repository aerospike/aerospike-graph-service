package com.aerospike.firefly.olap.codec.decoder;

import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.Set;

public class RowDecoderFactory {
    private static final RowDecoderFactory INSTANCE = new RowDecoderFactory();

    private RowDecoderFactory() {
    }

    public static RowDecoderFactory getInstance() {
        return INSTANCE;
    }


    public RowDecoder getDecoder(final Set<TraverserRequirement> requirements) {
        if (requirements.contains(TraverserRequirement.ONE_BULK)) {
            if (O_OB_S_SE_SL_RowDecoder.getInstance().getRequirements().containsAll(requirements))
                return O_OB_S_SE_SL_RowDecoder.getInstance();

            if (NL_O_OB_S_SE_SL_RowDecoder.getInstance().getRequirements().containsAll(requirements))
                return NL_O_OB_S_SE_SL_RowDecoder.getInstance();

            if (LP_O_OB_S_SE_SL_RowDecoder.getInstance().getRequirements().containsAll(requirements))
                return LP_O_OB_S_SE_SL_RowDecoder.getInstance();

            if (LP_NL_O_OB_S_SE_SL_RowDecoder.getInstance().getRequirements().containsAll(requirements))
                return LP_NL_O_OB_S_SE_SL_RowDecoder.getInstance();

            if (LP_O_OB_P_S_SE_SL_RowDecoder.getInstance().getRequirements().containsAll(requirements))
                return LP_O_OB_P_S_SE_SL_RowDecoder.getInstance();

            if (LP_NL_O_OB_P_S_SE_SL_RowDecoder.getInstance().getRequirements().containsAll(requirements))
                return LP_NL_O_OB_P_S_SE_SL_RowDecoder.getInstance();
        } else {
            if (B_O_RowDecoder.getInstance().getRequirements().containsAll(requirements))
                return B_O_RowDecoder.getInstance();

            if (B_O_S_SE_SL_RowDecoder.getInstance().getRequirements().containsAll(requirements))
                return B_O_S_SE_SL_RowDecoder.getInstance();

            if (B_NL_O_S_SE_SL_RowDecoder.getInstance().getRequirements().containsAll(requirements))
                return B_NL_O_S_SE_SL_RowDecoder.getInstance();

            if (B_LP_O_S_SE_SL_RowDecoder.getInstance().getRequirements().containsAll(requirements))
                return B_LP_O_S_SE_SL_RowDecoder.getInstance();

            if (B_LP_NL_O_S_SE_SL_RowDecoder.getInstance().getRequirements().containsAll(requirements))
                return B_LP_NL_O_S_SE_SL_RowDecoder.getInstance();

            if (B_LP_O_P_S_SE_SL_RowDecoder.getInstance().getRequirements().containsAll(requirements))
                return B_LP_O_P_S_SE_SL_RowDecoder.getInstance();

            if (B_LP_NL_O_P_S_SE_SL_RowDecoder.getInstance().getRequirements().containsAll(requirements))
                return B_LP_NL_O_P_S_SE_SL_RowDecoder.getInstance();
        }

        throw new IllegalStateException("The provided traverser generator factory does not support the requirements of the traversal: " + this.getClass().getCanonicalName() + requirements);
    }
}
