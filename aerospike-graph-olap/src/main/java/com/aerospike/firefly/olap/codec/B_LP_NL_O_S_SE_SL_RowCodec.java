package com.aerospike.firefly.olap.codec;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_NL_O_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceElement;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

import java.util.Set;

public class B_LP_NL_O_S_SE_SL_RowCodec extends RowCodec {
    private static final B_LP_NL_O_S_SE_SL_RowCodec INSTANCE = new B_LP_NL_O_S_SE_SL_RowCodec();

    public static B_LP_NL_O_S_SE_SL_RowCodec getInstance() {
        return INSTANCE;
    }

    private B_LP_NL_O_S_SE_SL_RowCodec() {
    }

    /////////////////////////////////////////////////////////////////////
    // Schema
    /////////////////////////////////////////////////////////////////////

    @Override
    StructType getSchema() {
        return getBaseSchema().
                add(BULK_COL, DataTypes.LongType, false);
    }

    /////////////////////////////////////////////////////////////////////
    // Decode
    /////////////////////////////////////////////////////////////////////

    @Override
    public Traverser decode(final Row row,
                            final TraverserGenerator traverserGenerator,
                            final TraversalMatrix traversalMatrix) {
        if (row.getString(row.fieldIndex(ID_COL)) == null) {
            throw new RuntimeException("Error, only rows with id col populated are currently supported");
        }
        final Object id = getId(row.getString(row.fieldIndex(ID_COL)), row.getInt(row.fieldIndex(ID_TYPEHINT_COL)));
        final String label = row.getString(row.fieldIndex(LABEL_COL));
        final String step = row.getString(row.fieldIndex(STEP_COL));
        final long bulk = row.getLong(row.fieldIndex(BULK_COL));
        final ReferenceElement element;
        if (row.getInt(row.fieldIndex(TRAVERSER_TYPE_COL)) == TRAVERSER_TYPE.VERTEX.ordinal()) {
            element = new ReferenceVertex(id, label);
        } else if (row.getInt(row.fieldIndex(TRAVERSER_TYPE_COL)) == TRAVERSER_TYPE.EDGE.ordinal()) {
            element = new ReferenceEdge(id, label, new ReferenceVertex("~empty"), new ReferenceVertex("~empty"));
        } else {
            throw new RuntimeException("Error, decoder for " + row.getInt(row.fieldIndex(TRAVERSER_TYPE_COL)) + " is not implemented");
        }
        final Traverser traverser = traverserGenerator.generate(element, traversalMatrix.getStepById(step), bulk);

        traverser.asAdmin().setStepId(step);
        return traverser;
    }

    /////////////////////////////////////////////////////////////////////
    // Encode
    /////////////////////////////////////////////////////////////////////

    @Override
    public Row encode(final Traverser traverser) {
        if (traverser.get() instanceof Vertex) {
            final Vertex vertex = (Vertex) traverser.get();
            return RowFactory.create(
                    TRAVERSER_TYPE.VERTEX.ordinal(), // Integer traverser type.
                    vertex.id().toString(), // String id.
                    getIdType(vertex.id()).ordinal(), // Integer ordinal.
                    vertex.label(), // String label.
                    traverser.asAdmin().isHalted(), // Boolean halted.
                    traverser.asAdmin().getStepId(), // String step.
                    traverser.bulk()); // Integer bulk.
        } else if (traverser.get() instanceof Edge) {
            final Edge edge = (Edge) traverser.get();
            return RowFactory.create(
                    TRAVERSER_TYPE.EDGE.ordinal(), // Integer traverser type.
                    edge.id().toString(), // String id.
                    getIdType(edge.id()).ordinal(), // Integer ordinal.
                    edge.label(), // String label.
                    traverser.asAdmin().isHalted(), // Boolean halted.
                    traverser.asAdmin().getStepId(), // String step.
                    traverser.bulk()); // Integer bulk.
        } else {
            throw new RuntimeException("Error, encoder for " + traverser.get().getClass() + " is not implemented");
        }
    }

    @Override
    public Row encode(final Vertex vertex, final String step) {
        return RowFactory.create(TRAVERSER_TYPE.VERTEX.ordinal(), vertex.id().toString(), getIdType(vertex.id()).ordinal(), vertex.label(), false, step, 1L);
    }

    @Override
    public Row encode(final Edge edge, final String step) {
        return RowFactory.create(TRAVERSER_TYPE.EDGE.ordinal(), edge.id().toString(), getIdType(edge.id()).ordinal(), edge.label(), false, step, 1L);
    }

    /////////////////////////////////////////////////////////////////////
    // Requirements
    /////////////////////////////////////////////////////////////////////

    @Override
    Set<TraverserRequirement> getRequirements() {
        return B_LP_NL_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements();
    }
}
