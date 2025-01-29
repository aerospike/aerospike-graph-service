package com.aerospike.firefly.olap.codec.encoder;

import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;

import java.util.Set;

public class RowEncoderFactory {
    private static final RowEncoderFactory INSTANCE = new RowEncoderFactory();

    private RowEncoderFactory() {
    }

    public static RowEncoderFactory getInstance() {
        return INSTANCE;
    }

    public RowEncoder getEncoder(final Set<TraverserRequirement> requirements) {
        if (requirements.contains(TraverserRequirement.ONE_BULK)) {
            if (O_OB_S_SE_SL_RowEncoder.getInstance().getRequirements().containsAll(requirements))
                return O_OB_S_SE_SL_RowEncoder.getInstance();

            if (NL_O_OB_S_SE_SL_RowEncoder.getInstance().getRequirements().containsAll(requirements))
                return NL_O_OB_S_SE_SL_RowEncoder.getInstance();

            if (LP_O_OB_S_SE_SL_RowEncoder.getInstance().getRequirements().containsAll(requirements))
                return LP_O_OB_S_SE_SL_RowEncoder.getInstance();

            if (LP_NL_O_OB_S_SE_SL_RowEncoder.getInstance().getRequirements().containsAll(requirements))
                return LP_NL_O_OB_S_SE_SL_RowEncoder.getInstance();

            if (LP_O_OB_P_S_SE_SL_RowEncoder.getInstance().getRequirements().containsAll(requirements))
                return LP_O_OB_P_S_SE_SL_RowEncoder.getInstance();

            if (LP_NL_O_OB_P_S_SE_SL_RowEncoder.getInstance().getRequirements().containsAll(requirements))
                return LP_NL_O_OB_P_S_SE_SL_RowEncoder.getInstance();
        } else {
            if (B_O_RowEncoder.getInstance().getRequirements().containsAll(requirements))
                return B_O_RowEncoder.getInstance();

            if (B_O_S_SE_SL_RowEncoder.getInstance().getRequirements().containsAll(requirements))
                return B_O_S_SE_SL_RowEncoder.getInstance();

            if (B_NL_O_S_SE_SL_RowEncoder.getInstance().getRequirements().containsAll(requirements))
                return B_NL_O_S_SE_SL_RowEncoder.getInstance();

            if (B_LP_O_S_SE_SL_RowEncoder.getInstance().getRequirements().containsAll(requirements))
                return B_LP_O_S_SE_SL_RowEncoder.getInstance();

            if (B_LP_NL_O_S_SE_SL_RowEncoder.getInstance().getRequirements().containsAll(requirements))
                return B_LP_NL_O_S_SE_SL_RowEncoder.getInstance();

            if (B_LP_O_P_S_SE_SL_RowEncoder.getInstance().getRequirements().containsAll(requirements))
                return B_LP_O_P_S_SE_SL_RowEncoder.getInstance();

            if (B_LP_NL_O_P_S_SE_SL_RowEncoder.getInstance().getRequirements().containsAll(requirements))
                return B_LP_NL_O_P_S_SE_SL_RowEncoder.getInstance();
        }

        throw new IllegalStateException("The provided traverser generator factory does not support the requirements of the traversal: " + this.getClass().getCanonicalName() + requirements);
    }
}
