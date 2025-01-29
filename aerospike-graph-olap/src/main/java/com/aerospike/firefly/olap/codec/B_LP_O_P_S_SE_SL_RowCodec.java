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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class B_LP_O_P_S_SE_SL_RowCodec extends RowCodec {
    private static final B_LP_O_P_S_SE_SL_RowCodec INSTANCE = new B_LP_O_P_S_SE_SL_RowCodec();

    public static B_LP_O_P_S_SE_SL_RowCodec getInstance() {
        return INSTANCE;
    }

    private B_LP_O_P_S_SE_SL_RowCodec() {
    }

    /////////////////////////////////////////////////////////////////////
    // Schema
    /////////////////////////////////////////////////////////////////////

    @Override
    StructType getSchema() {
        return getBaseSchema()
                .add(BULK_COL, DataTypes.LongType, false)
                .add(PATH_ID_COL, DataTypes.createArrayType(DataTypes.StringType), true)
                .add(PATH_ID_TYPEHINT_COL, DataTypes.createArrayType(DataTypes.IntegerType), true)
                .add(PATH_OBJ_TYPE_COL, DataTypes.createArrayType(DataTypes.IntegerType), true)
                .add(PATH_LABELS_COL, DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.StringType)), true);
    }

    /////////////////////////////////////////////////////////////////////
    // Decode
    /////////////////////////////////////////////////////////////////////

    @Override
    Traverser decode(final Row row, final TraverserGenerator tg, final TraversalMatrix tm) {
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
        final List<String> pathIds = row.getList(row.fieldIndex(PATH_ID_COL));
        final List<Integer> pathIdTypeHints = row.getList(row.fieldIndex(PATH_ID_TYPEHINT_COL));
        final List<Integer> pathObjTypes = row.getList(row.fieldIndex(PATH_OBJ_TYPE_COL));
        final List<Seq<String>> pathLabels = row.getList(row.fieldIndex(PATH_LABELS_COL));

        final _B_LP_O_P_S_SE_SL_Traverser<?> pathTraverser = new _B_LP_O_P_S_SE_SL_Traverser<>(element, tm.getStepById(step), bulk);
        pathTraverser.setStepId(step);
        if (pathIds != null) {
            pathTraverser.setPath(pathIds, pathIdTypeHints, pathObjTypes, pathLabels);
        }
        return pathTraverser;
    }

    static class _B_LP_O_P_S_SE_SL_Traverser<T> extends B_LP_O_P_S_SE_SL_Traverser<T> {
        public _B_LP_O_P_S_SE_SL_Traverser(final T t, final Step<T, ?> step, final long initialBulk) {
            super(t, step, initialBulk);
        }

        public void setPath(final List<String> pathIds,
                            final List<Integer> pathIdTypeHints,
                            final List<Integer> pathObjTypes,
                            final List<Seq<String>> pathLabels) {
            this.path = ImmutablePath.make();
            for (int i = 0; i < pathIds.size(); i++) {
                final Element e = getReferenceElement(pathObjTypes.get(i), getId(pathIds.get(i), pathIdTypeHints.get(i)), null);
                final Set<String> labels = new HashSet<>(JavaConverters.seqAsJavaListConverter(pathLabels.get(i)).asJava());
                this.path = this.path.extend(e, labels);
            }
        }
    }

    /////////////////////////////////////////////////////////////////////
    // Encode
    /////////////////////////////////////////////////////////////////////

    @Override
    Row encode(final Traverser traverser) {
        System.out.println("Traverser encode " + traverser.asAdmin().isHalted());
        final Path path = traverser.path();
        final List<String> ids = new ArrayList<>();
        final List<Integer> idTypeHints = new ArrayList<>();
        final List<Integer> objTypes = new ArrayList<>();
        final List<Seq<String>> labelsList = new ArrayList<>();
        for (final Object o : path.objects()) {
            if (o instanceof Element) {
                final Element e = (Element) o;
                ids.add(e.id().toString());
                idTypeHints.add(getIdType(e.id()).ordinal());
                if (e instanceof Vertex) {
                    objTypes.add(TRAVERSER_TYPE.VERTEX.ordinal());
                } else if (e instanceof Edge) {
                    objTypes.add(TRAVERSER_TYPE.EDGE.ordinal());
                } else {
                    throw new RuntimeException("Error, only elements are currently supported");
                }
            } else {
                throw new RuntimeException("Error, only elements are currently supported");
            }
        }
        for (final Set<String> labels : path.labels()) {
            labelsList.add(JavaConverters.asScalaBufferConverter(new ArrayList<>(labels)).asScala());
        }
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
                    JavaConverters.asScalaBufferConverter(ids).asScala(),
                    JavaConverters.asScalaBufferConverter(idTypeHints).asScala(),
                    JavaConverters.asScalaBufferConverter(objTypes).asScala(),
                    JavaConverters.asScalaBufferConverter(labelsList).asScala());
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
                    JavaConverters.asScalaBufferConverter(ids).asScala(),
                    JavaConverters.asScalaBufferConverter(idTypeHints).asScala(),
                    JavaConverters.asScalaBufferConverter(objTypes).asScala(),
                    JavaConverters.asScalaBufferConverter(labelsList).asScala());
        } else {
            throw new RuntimeException("Error, encoder for " + traverser.get().getClass() + " is not implemented");
        }
    }

    @Override
    public Row encode(final Vertex vertex, final String step) {
        return RowFactory.create(
                TRAVERSER_TYPE.VERTEX.ordinal(),
                vertex.id().toString(),
                getIdType(vertex.id()).ordinal(),
                vertex.label(),
                false,
                step,
                1L,
                null,
                null,
                null,
                null);
    }

    @Override
    public Row encode(final Edge edge, final String step) {
        return RowFactory.create(
                TRAVERSER_TYPE.EDGE.ordinal(),
                edge.id().toString(),
                getIdType(edge.id()).ordinal(),
                edge.label(),
                false,
                step,
                1L,
                null,
                null,
                null,
                null);
    }

    /////////////////////////////////////////////////////////////////////
    // Requirements
    /////////////////////////////////////////////////////////////////////

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return B_LP_O_P_S_SE_SL_TraverserGenerator.instance().getProvidedRequirements();
    }
}
