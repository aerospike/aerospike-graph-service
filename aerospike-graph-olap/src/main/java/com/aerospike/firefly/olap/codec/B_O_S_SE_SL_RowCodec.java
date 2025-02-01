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

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class B_O_S_SE_SL_RowCodec extends RowCodec {
    private static final B_O_S_SE_SL_RowCodec INSTANCE = new B_O_S_SE_SL_RowCodec();

    public static B_O_S_SE_SL_RowCodec getInstance() {
        return INSTANCE;
    }

    /////////////////////////////////////////////////////////////////////
    // Schema
    /////////////////////////////////////////////////////////////////////

    @Override
    StructType getSchema() {
        return getBaseSchema().
                add(BULK_COL, DataTypes.LongType, false)
                .add(SL_COUNT_COL, DataTypes.IntegerType, true)
                .add(SL_NAME_COL, DataTypes.StringType, true);
    }

    /////////////////////////////////////////////////////////////////////
    // Decode
    /////////////////////////////////////////////////////////////////////
    @Override
    Traverser decode(final Row row,
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
        final String slName = row.isNullAt(row.fieldIndex(SL_NAME_COL)) ? null : row.getString(row.fieldIndex(SL_NAME_COL));
        final Integer slCount = row.isNullAt(row.fieldIndex(SL_COUNT_COL)) ? 0 : row.getInt(row.fieldIndex(SL_COUNT_COL));
        final Step stepOrEmpty = Optional.ofNullable(traversalMatrix.getStepById(step)).orElse(EmptyStep.instance());
        final Traverser traverser = new _B_O_S_SE_SL_Traverser<>(element, stepOrEmpty, bulk);
        if (slName != null) {
            // Step label ignored unless nested loop.
            traverser.asAdmin().initialiseLoops(null, slName);
        }
        for (int i = 0; i < slCount; i++) {
            traverser.asAdmin().incrLoops();
        }
        traverser.asAdmin().setStepId(step);
        return traverser;
    }

    /////////////////////////////////////////////////////////////////////
    // Encode
    /////////////////////////////////////////////////////////////////////

    @Override
    public Row encode(final Traverser traverser) {
        final _B_O_S_SE_SL_Traverser<?> traverserAdmin = (_B_O_S_SE_SL_Traverser<?>) traverser.asAdmin();
        final String loop = traverserAdmin.getLoopName();
        final int loopCount = traverserAdmin.getLoopCount();
        if (traverser.get() instanceof Vertex) {
            final Vertex vertex = (Vertex) traverser.get();
            return RowFactory.create(
                    TRAVERSER_TYPE.VERTEX.ordinal(), // Integer traverser type.
                    vertex.id().toString(), // String id.
                    getIdType(vertex.id()).ordinal(), // Integer ordinal.
                    vertex.label(), // String label.
                    traverser.asAdmin().isHalted(), // Boolean halted.
                    traverser.asAdmin().getStepId(), // String step.
                    traverser.bulk(), // Integer bulk.
                    loopCount, // Loop count for single loop.
                    loop);// Loop name for single loop.
        } else if (traverser.get() instanceof Edge) {
            final Edge edge = (Edge) traverser.get();
            return RowFactory.create(
                    TRAVERSER_TYPE.EDGE.ordinal(), // Integer traverser type.
                    edge.id().toString(), // String id.
                    getIdType(edge.id()).ordinal(), // Integer ordinal.
                    edge.label(), // String label.
                    traverser.asAdmin().isHalted(), // Boolean halted.
                    traverser.asAdmin().getStepId(), // String step.
                    traverser.bulk(), // Integer bulk.
                    loopCount, // Loop count for single loop.
                    loop);// Loop name for single loop.
        } else {
            throw new RuntimeException("Error, encoder for " + traverser.get().getClass() + " is not implemented");
        }
    }

    @Override
    public Row encode(final Vertex vertex, final String step) {
        return RowFactory.create(TRAVERSER_TYPE.VERTEX.ordinal(), vertex.id().toString(), getIdType(vertex.id()).ordinal(), vertex.label(), false, step, 1L, null, null);
    }

    @Override
    public Row encode(final Edge edge, final String step) {
        return RowFactory.create(TRAVERSER_TYPE.EDGE.ordinal(), edge.id().toString(), getIdType(edge.id()).ordinal(), edge.label(), false, step, 1L, null, null);
    }

    static class _B_O_S_SE_SL_Traverser<T> extends B_O_S_SE_SL_Traverser<T> {
        public _B_O_S_SE_SL_Traverser(final T t, final Step<T, ?> step, final long initialBulk) {
            super(t, step, initialBulk);
        }

        public String getLoopName() {
            return loopName;
        }

        public int getLoopCount() {
            return loops;
        }
    }

    /////////////////////////////////////////////////////////////////////
    // Requirements
    /////////////////////////////////////////////////////////////////////

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return B_O_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements();
    }
}
