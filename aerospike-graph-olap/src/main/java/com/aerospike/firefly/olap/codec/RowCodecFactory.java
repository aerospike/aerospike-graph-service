/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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


    public RowCodec getCodec(final Set<TraverserRequirement> requirements, final int nativeSparkOperationColumns) {
        if (requirements.contains(TraverserRequirement.ONE_BULK)) {
            if (O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.SINGLE_LOOP),
                        requirements, nativeSparkOperationColumns);

            if (NL_O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.NESTED_LOOP,
                                RowCodec.CodecRequirements.SINGLE_LOOP),
                        requirements, nativeSparkOperationColumns);

            if (LP_O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.PATH,
                                RowCodec.CodecRequirements.SINGLE_LOOP),
                        requirements, nativeSparkOperationColumns);

            if (LP_NL_O_OB_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.PATH,

                                RowCodec.CodecRequirements.SINGLE_LOOP,
                                RowCodec.CodecRequirements.NESTED_LOOP),
                        requirements, nativeSparkOperationColumns);

            if (LP_O_OB_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.PATH,
                                RowCodec.CodecRequirements.SINGLE_LOOP),
                        requirements, nativeSparkOperationColumns);

            if (LP_NL_O_OB_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.PATH,
                                RowCodec.CodecRequirements.SINGLE_LOOP,
                                RowCodec.CodecRequirements.NESTED_LOOP),
                        requirements, nativeSparkOperationColumns);
        } else {
            if (B_O_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.BULK), requirements, nativeSparkOperationColumns);

            if (B_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.BULK,
                                RowCodec.CodecRequirements.SINGLE_LOOP),
                        requirements, nativeSparkOperationColumns);

            if (B_NL_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.BULK,
                                RowCodec.CodecRequirements.SINGLE_LOOP,
                                RowCodec.CodecRequirements.NESTED_LOOP),
                        requirements, nativeSparkOperationColumns);

            if (B_LP_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.BULK,
                                RowCodec.CodecRequirements.SINGLE_LOOP,
                                RowCodec.CodecRequirements.PATH), requirements, nativeSparkOperationColumns);

            if (B_LP_NL_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.BULK,
                                RowCodec.CodecRequirements.SINGLE_LOOP,
                                RowCodec.CodecRequirements.NESTED_LOOP),
                        requirements, nativeSparkOperationColumns);

            if (B_LP_O_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.BULK,
                                RowCodec.CodecRequirements.SINGLE_LOOP,
                                RowCodec.CodecRequirements.PATH), requirements, nativeSparkOperationColumns);

            if (B_LP_NL_O_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements().containsAll(requirements))
                return new RowCodec(
                        List.of(RowCodec.CodecRequirements.BASE,
                                RowCodec.CodecRequirements.BULK,
                                RowCodec.CodecRequirements.PATH,
                                RowCodec.CodecRequirements.SINGLE_LOOP,
                                RowCodec.CodecRequirements.NESTED_LOOP),
                        requirements, nativeSparkOperationColumns);
        }

        throw new IllegalStateException("The provided traverser generator factory does not support the requirements of the traversal: " + this.getClass().getCanonicalName() + requirements);
    }
}
