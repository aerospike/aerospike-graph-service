package com.aerospike.firefly.olap.codec;

import com.aerospike.firefly.olap.structure.DistributedTraversalMatrix;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_TraverserGenerator;
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

import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.aerospike.firefly.olap.codec.RowCodecHelper.getId;
import static com.aerospike.firefly.olap.codec.RowCodecHelper.getReferenceElement;

public class B_LP_O_P_S_SE_SL_RowCodec extends RowCodec {
    private static final B_LP_O_P_S_SE_SL_RowCodec INSTANCE = new B_LP_O_P_S_SE_SL_RowCodec();

    public static B_LP_O_P_S_SE_SL_RowCodec getInstance() {
        return INSTANCE;
    }

    private B_LP_O_P_S_SE_SL_RowCodec() {
        super(List.of(CodecRequirements.BASE, CodecRequirements.BULK, CodecRequirements.SINGLE_LOOP, CodecRequirements.PATH));
    }

    /////////////////////////////////////////////////////////////////////
    // Requirements
    /////////////////////////////////////////////////////////////////////

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return B_LP_O_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements();
    }
}
