package com.aerospike.firefly.olap.codec;

import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_NL_O_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_NL_O_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_NL_O_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_NL_O_OB_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_NL_O_OB_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.NL_O_OB_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.O_OB_S_SE_SL_TraverserGenerator;
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
            if (O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.SINGLE_LOOP),
                        requirements);

            if (NL_O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.NESTED_LOOP,
                                RowCodec.CodecRequirements.SINGLE_LOOP),
                        requirements);

            if (LP_O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.PATH,
                                RowCodec.CodecRequirements.SINGLE_LOOP),
                        requirements);

            if (LP_NL_O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.PATH,

                                RowCodec.CodecRequirements.SINGLE_LOOP,
                                RowCodec.CodecRequirements.NESTED_LOOP),
                        requirements);

            if (LP_O_OB_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.PATH,
                                RowCodec.CodecRequirements.SINGLE_LOOP),
                        requirements);

            if (LP_NL_O_OB_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.PATH,
                                RowCodec.CodecRequirements.SINGLE_LOOP,
                                RowCodec.CodecRequirements.NESTED_LOOP),
                        requirements);
        } else {
            if (B_O_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.BULK), requirements);

            if (B_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.BULK,
                                RowCodec.CodecRequirements.SINGLE_LOOP),
                        requirements);

            if (B_NL_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.BULK,
                                RowCodec.CodecRequirements.SINGLE_LOOP,
                                RowCodec.CodecRequirements.NESTED_LOOP),
                        requirements);

            if (B_LP_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.BULK,
                                RowCodec.CodecRequirements.SINGLE_LOOP,
                                RowCodec.CodecRequirements.PATH), requirements);

            if (B_LP_NL_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.BULK,
                                RowCodec.CodecRequirements.SINGLE_LOOP,
                                RowCodec.CodecRequirements.NESTED_LOOP),
                        requirements);

            if (B_LP_O_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.BULK,
                                RowCodec.CodecRequirements.SINGLE_LOOP,
                                RowCodec.CodecRequirements.PATH), requirements);

            if (B_LP_NL_O_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.BULK,
                                RowCodec.CodecRequirements.PATH,
                                RowCodec.CodecRequirements.SINGLE_LOOP,
                                RowCodec.CodecRequirements.NESTED_LOOP),
                        requirements);
        }

        throw new IllegalStateException("The provided traverser generator factory does not support the requirements of the traversal: " + this.getClass().getCanonicalName() + requirements);
    }
}
