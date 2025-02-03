package com.aerospike.firefly.olap.codec;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceElement;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

import java.util.List;
import java.util.Set;

import static com.aerospike.firefly.olap.codec.RowCodecHelper.getId;

public class B_O_RowCodec extends RowCodec {
    private static final B_O_RowCodec INSTANCE = new B_O_RowCodec();

    public static B_O_RowCodec getInstance() {
        return INSTANCE;
    }

    private B_O_RowCodec() {
        super(List.of(CodecRequirements.BASE, CodecRequirements.BULK));
    }

    /////////////////////////////////////////////////////////////////////
    // Requirements
    /////////////////////////////////////////////////////////////////////
    @Override
    public Set<TraverserRequirement> getRequirements() {
        return B_O_TraverserGenerator.instance().getProvidedRequirements();
    }
}
