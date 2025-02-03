package com.aerospike.firefly.olap.codec;

import io.grpc.InternalGlobalInterceptors;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceElement;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aerospike.firefly.olap.codec.RowCodecHelper.getId;

public class B_O_S_SE_SL_RowCodec extends RowCodec {
    private static B_O_S_SE_SL_RowCodec INSTANCE = new B_O_S_SE_SL_RowCodec();

    private B_O_S_SE_SL_RowCodec() {
        super(List.of(CodecRequirements.BASE, CodecRequirements.BULK, CodecRequirements.SINGLE_LOOP));
    }

    public static B_O_S_SE_SL_RowCodec getInstance() {
        return INSTANCE;
    }

    /////////////////////////////////////////////////////////////////////
    // Requirements
    /////////////////////////////////////////////////////////////////////

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return B_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements();
    }
}
