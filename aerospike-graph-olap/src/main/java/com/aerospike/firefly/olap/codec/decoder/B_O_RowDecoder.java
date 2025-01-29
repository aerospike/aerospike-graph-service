package com.aerospike.firefly.olap.codec.decoder;

import com.aerospike.firefly.olap.codec.Codec;
import com.aerospike.firefly.olap.codec.schema.RowSchema;
import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceElement;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

import java.util.Set;

import static com.aerospike.firefly.olap.codec.Codec.getId;

public class B_O_RowDecoder implements RowDecoder {
    private static final B_O_RowDecoder INSTANCE = new B_O_RowDecoder();

    public static B_O_RowDecoder getInstance() {
        return INSTANCE;
    }

    private B_O_RowDecoder() {
    }

    public Traverser decode(final Row row,
                                   final TraverserGenerator traverserGenerator,
                                   final TraversalMatrix traversalMatrix) {
        if (row.getString(row.fieldIndex(RowSchema.ID_COL)) == null) {
            throw new RuntimeException("Error, only rows with id col populated are currently supported");
        }
        final Object id = getId(row.getString(row.fieldIndex(RowSchema.ID_COL)), row.getInt(row.fieldIndex(RowSchema.ID_TYPEHINT_COL)));
        final String label = row.getString(row.fieldIndex(RowSchema.LABEL_COL));
        final String step = row.getString(row.fieldIndex(RowSchema.STEP_COL));
        final long bulk = row.getLong(row.fieldIndex(RowSchema.BULK_COL));
        final ReferenceElement element;
        if (row.getInt(row.fieldIndex(RowSchema.TRAVERSER_TYPE_COL)) == Codec.TRAVERSER_TYPE.VERTEX.ordinal()) {
            element = new ReferenceVertex(id, label);
        } else if (row.getInt(row.fieldIndex(RowSchema.TRAVERSER_TYPE_COL)) == Codec.TRAVERSER_TYPE.EDGE.ordinal()) {
            element = new ReferenceEdge(id, label, null, null);
        } else {
            throw new RuntimeException("Error, decoder for " + row.getInt(row.fieldIndex(RowSchema.TRAVERSER_TYPE_COL)) + " is not implemented");
        }
        final Traverser traverser = traverserGenerator.generate(element, traversalMatrix.getStepById(step), bulk);
        traverser.asAdmin().setStepId(step);
        return traverser;
    }

    public Set<TraverserRequirement> getRequirements() {
        return B_O_TraverserGenerator.instance().getProvidedRequirements();
    }
}
