package com.aerospike.firefly.olap.codec;

import com.aerospike.firefly.olap.structure.DistributedReferenceEdgeProperty;
import com.aerospike.firefly.olap.structure.DistributedReferenceVertexProperty;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceElement;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertexProperty;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.aerospike.firefly.olap.codec.RowCodecHelper.appendNestedLoopSchema;
import static com.aerospike.firefly.olap.codec.RowCodecHelper.appendPathSchema;
import static com.aerospike.firefly.olap.codec.RowCodecHelper.appendSingleLoopSchema;
import static com.aerospike.firefly.olap.codec.RowCodecHelper.getId;
import static com.aerospike.firefly.olap.codec.RowCodecHelper.getReferenceElement;
import static com.aerospike.firefly.olap.codec.RowCodecHelper.setNestedLoops;
import static com.aerospike.firefly.olap.codec.RowCodecHelper.setPath;

public abstract class RowCodec {

    public Map<String, Integer> columnToOrdinal = new HashMap<>();

    final List<CodecRequirements> codecRequirements;
    final Set<CodecRequirements> codecRequirementsSet = new HashSet<>();

    interface TraverserEncoder {
        void encode(final List<Object> o, final Traverser t);
    }

    interface ElementEncoder {
        void encode(final List<Object> o, final TraversalMatrix tm, final Element e, final String step);
    }

    final List<TraverserEncoder> orderedTraverserEncoders = new ArrayList<>();
    final List<ElementEncoder> orderedElementEncoders = new ArrayList<>();

    RowCodec(final List<CodecRequirements> requirements) {
        this.codecRequirements = requirements;
        int i = 0;
        for (CodecRequirements requirement : requirements) {
            switch (requirement) {
                case BASE:
                    orderedTraverserEncoders.add(new BaseTraverserEncoder());
                    orderedElementEncoders.add(new BaseElementEncoder());
                    columnToOrdinal.put(TRAVERSER_TYPE_COL, i++);
                    columnToOrdinal.put(ELEMENT_ID_COL, i++);
                    columnToOrdinal.put(ELEMENT_ID_TYPEHINT_COL, i++);
                    columnToOrdinal.put(ID_COL, i++);
                    columnToOrdinal.put(ID_TYPEHINT_COL, i++);
                    columnToOrdinal.put(LABEL_COL, i++);
                    columnToOrdinal.put(HALTED_COL, i++);
                    columnToOrdinal.put(STEP_COL, i++);
                    break;
                case BULK:
                    orderedTraverserEncoders.add(new BulkTraverserEncoder());
                    orderedElementEncoders.add(new BulkElementEncoder());
                    columnToOrdinal.put(BULK_COL, i++);
                    break;
                case SINGLE_LOOP:
                    orderedTraverserEncoders.add(new SingleLoopTraverserEncoder());
                    orderedElementEncoders.add(new SingleLoopElementEncoder());
                    columnToOrdinal.put(TRAVERSER_TYPE_COL, columnToOrdinal.size());
                    columnToOrdinal.put(SL_COUNT_COL, i++);
                    columnToOrdinal.put(SL_NAME_COL, i++);
                    break;
                case PATH:
                    orderedTraverserEncoders.add(new PathTraverserEncoder());
                    orderedElementEncoders.add(new PathElementEncoder());
                    columnToOrdinal.put(PATH_ID_COL, i++);
                    columnToOrdinal.put(PATH_ID_TYPEHINT_COL, i++);
                    columnToOrdinal.put(PATH_OBJ_TYPE_COL, i++);
                    columnToOrdinal.put(PATH_LABELS_COL, i++);
                    break;
                case NESTED_LOOP:
                    orderedTraverserEncoders.add(new NestedLoopTraverserEncoder());
                    orderedElementEncoders.add(new NestedLoopElementEncoder());
                    columnToOrdinal.put(NL_COUNT_COL, i++);
                    columnToOrdinal.put(NL_NAME_COL, i++);
                    columnToOrdinal.put(NL_STEP_COL, i++);
            }
        }
        codecRequirementsSet.addAll(requirements);
    }

    /////////////////////////////////////////////////////////////////////
    // Columns
    /////////////////////////////////////////////////////////////////////

    public static final String TRAVERSER_TYPE_COL = "~traverser_type";
    public static final String ID_COL = "~id";
    public static final String ID_TYPEHINT_COL = "~id_typehint";
    public static final String LABEL_COL = "~label";
    public static final String ELEMENT_ID_COL = "~eid";
    public static final String ELEMENT_ID_TYPEHINT_COL = "~eid_typehint";
    public static final String VERTEX_ID_COL = "~vertex_id"; // ONLY FOR VertexPropery TRAVERSERS
    public static final String VERTEX_ID_TYPEHINT_COL = "~vertex_id_typehint"; // ONLY FOR VertexPropery TRAVERSERS
    public static final String PROPERTIES_COL = "~properties";
    public static final String IN_COL = "~in";
    public static final String OUT_COL = "~out";
    public static final String HALTED_COL = "~halted";
    public static final String REF_COL = "~ref";
    public static final String STEP_COL = "~step";
    public static final String BULK_COL = "~bulk";
    public static final String SL_COUNT_COL = "~slc";
    public static final String SL_NAME_COL = "~sln";
    public static final String NL_STEP_COL = "~nl_step";
    public static final String NL_COUNT_COL = "~nl_count";
    public static final String NL_NAME_COL = "~nl_name";
    public static final String PATH_ID_COL = "~path_id";
    public static final String PATH_ID_TYPEHINT_COL = "~path_id_typehint";
    public static final String PATH_OBJ_TYPE_COL = "~path_obj_type";
    public static final String PATH_LABELS_COL = "~path_steps";

    static class BulkTraverserEncoder implements TraverserEncoder {
        @Override
        public void encode(final List<Object> o, final Traverser t) {
            RowCodecHelper.addBulk(o, t);
        }
    }

    class SingleLoopTraverserEncoder implements TraverserEncoder {
        @Override
        public void encode(final List<Object> o, final Traverser t) {
            RowCodecHelper.SingleLoopInfo sli = RowCodecHelper.getSingleLoopInfo(t);
            RowCodecHelper.addSingleLoop(o, sli);
        }
    }

    class BaseTraverserEncoder implements TraverserEncoder {
        @Override
        public void encode(final List<Object> o, final Traverser t) {
            RowCodecHelper.addBaseRow(o, t);
        }
    }

    class PathTraverserEncoder implements TraverserEncoder {
        @Override
        public void encode(final List<Object> o, final Traverser t) {
            final RowCodecHelper.PathInfo pi = RowCodecHelper.getPathInfo(t);
            RowCodecHelper.addPath(o, pi);
        }
    }

    class NestedLoopTraverserEncoder implements TraverserEncoder {
        @Override
        public void encode(final List<Object> o, final Traverser t) {
            final RowCodecHelper.NestedLoopInfo nli = RowCodecHelper.getNestedLoopInfo(t);
            RowCodecHelper.addNestedLoops(o, nli);
        }
    }

    class BaseElementEncoder implements ElementEncoder {
        @Override
        public void encode(final List<Object> o, TraversalMatrix tm, final Element e, final String step) {
            RowCodecHelper.addBaseRow(o, tm, e, step);
        }
    }

    class PathElementEncoder implements ElementEncoder {
        @Override
        public void encode(final List<Object> o, final TraversalMatrix tm, final Element e, final String step) {
            final Step tStep = tm.getStepById(step);
            final Step previousStep = tStep.getPreviousStep();
            final Set<String> labels = previousStep.getLabels();
            final List<Seq<String>> pathLabels = new ArrayList<>();
            if (labels != null && !labels.isEmpty()) {
                pathLabels.add(JavaConverters.asScalaBufferConverter(new ArrayList<>(labels)).asScala().seq());
            }
            o.add(null);
            o.add(null);
            o.add(null);
            o.add(pathLabels.isEmpty() ? null : JavaConverters.asScalaBufferConverter((pathLabels)).asScala().seq());
        }
    }

    class SingleLoopElementEncoder implements ElementEncoder {
        @Override
        public void encode(final List<Object> o, TraversalMatrix tm, final Element e, final String step) {
            o.add(null);
            o.add(null);
        }
    }

    class BulkElementEncoder implements ElementEncoder {
        @Override
        public void encode(final List<Object> o, TraversalMatrix tm, final Element e, final String step) {
            o.add(1L);
        }
    }

    class NestedLoopElementEncoder implements ElementEncoder {
        @Override
        public void encode(final List<Object> o, TraversalMatrix tm, final Element e, final String step) {
            o.add(null);
            o.add(null);
            o.add(null);
        }
    }

    public StructType getSchema() {
        StructType schema = RowCodecHelper.getBaseSchema();
        for (final CodecRequirements requirements : codecRequirements) {
            switch (requirements) {
                case BULK:
                    schema = schema.add(BULK_COL, DataTypes.LongType, false);
                    break;
                case SINGLE_LOOP:
                    schema = appendSingleLoopSchema(schema);
                    break;
                case PATH:
                    schema = appendPathSchema(schema);
                    break;
                case NESTED_LOOP:
                    schema = appendNestedLoopSchema(schema);
                    break;
            }
        }
        return schema;
    }

    /////////////////////////////////////////////////////////////////////
    // Decode
    /////////////////////////////////////////////////////////////////////

    Traverser decode(final Row row, final TraverserGenerator tg, final TraversalMatrix tm) {
        if (row.getString(row.fieldIndex(ID_COL)) == null) {
            throw new RuntimeException("Error, only rows with id col populated are currently supported");
        }
        final Object id = getId(row.getString(row.fieldIndex(ID_COL)), row.getInt(row.fieldIndex(ID_TYPEHINT_COL)));
        final String label = row.getString(row.fieldIndex(LABEL_COL));
        final String step = row.getString(row.fieldIndex(STEP_COL));
        final Step stepOrEmpty = Optional.ofNullable(tm.getStepById(step)).orElse(EmptyStep.instance());
        final long bulk = codecRequirementsSet.contains(CodecRequirements.BULK) ? row.getLong(row.fieldIndex(BULK_COL)) : 1L;
        final int traverserType = row.getInt(row.fieldIndex(TRAVERSER_TYPE_COL));
        final Object element;
        if (traverserType == TRAVERSER_TYPE.VERTEX.ordinal()) {
            element = new ReferenceVertex(id, label);
        } else if (traverserType == TRAVERSER_TYPE.EDGE.ordinal()) {
            element = new ReferenceEdge(id, label, new ReferenceVertex("~empty"), new ReferenceVertex("~empty"));
        } else if (traverserType == TRAVERSER_TYPE.VERTEX_PROPERTY.ordinal()){
            Object elementId = row.get(row.fieldIndex(ELEMENT_ID_COL));
            final int elementIdTypehint = row.getInt(row.fieldIndex(ELEMENT_ID_TYPEHINT_COL));
            elementId = getId(elementId.toString(), elementIdTypehint); // TODO: Update.
            element = new DistributedReferenceVertexProperty(id, elementId);
        } else if (traverserType == TRAVERSER_TYPE.EDGE_PROPERTY.ordinal()) {
            Object elementId = row.get(row.fieldIndex(ELEMENT_ID_COL));
            final int elementIdTypehint = row.getInt(row.fieldIndex(ELEMENT_ID_TYPEHINT_COL));
            elementId = getId(elementId.toString(), elementIdTypehint); // TODO: Update.
            element = new DistributedReferenceEdgeProperty<>(id.toString(), (ReferenceEdge) getReferenceElement(TRAVERSER_TYPE.EDGE.ordinal(), elementId, "~empty"));
        } else {
            throw new RuntimeException("Error, decoder for " + row.getInt(row.fieldIndex(TRAVERSER_TYPE_COL)) + " is not implemented");
        }
        final Traverser traverser = tg.generate(element, stepOrEmpty, bulk);
        traverser.asAdmin().setStepId(step);
        if (codecRequirementsSet.contains(CodecRequirements.SINGLE_LOOP)) {
            final String slName = row.isNullAt(row.fieldIndex(SL_NAME_COL)) ? null : row.getString(row.fieldIndex(SL_NAME_COL));
            final Integer slCount = row.isNullAt(row.fieldIndex(SL_COUNT_COL)) ? 0 : row.getInt(row.fieldIndex(SL_COUNT_COL));
            if (slName != null) {
                traverser.asAdmin().initialiseLoops(null, slName);
            }
            for (int i = 0; i < slCount; i++) {
                traverser.asAdmin().incrLoops();
            }
        }
        if (codecRequirementsSet.contains(CodecRequirements.PATH)) {
            final List<String> pathIds = row.getList(row.fieldIndex(PATH_ID_COL));
            final List<Integer> pathIdTypeHints = row.getList(row.fieldIndex(PATH_ID_TYPEHINT_COL));
            final List<Integer> pathObjTypes = row.getList(row.fieldIndex(PATH_OBJ_TYPE_COL));
            final List<Seq<String>> pathLabels = row.getList(row.fieldIndex(PATH_LABELS_COL));
            setPath(traverser, new RowCodecHelper.PathInfo(pathIds, pathIdTypeHints, pathObjTypes, pathLabels));
        }
        if (codecRequirementsSet.contains(CodecRequirements.NESTED_LOOP)) {
            final List<String> nlSteps = row.getList(row.fieldIndex(NL_STEP_COL));
            final List<String> nlNames = row.getList(row.fieldIndex(NL_NAME_COL));
            final List<Integer> nlCounts = row.getList(row.fieldIndex(NL_COUNT_COL));
            setNestedLoops(traverser, new RowCodecHelper.NestedLoopInfo(nlNames, nlCounts, nlSteps));
        }

        if (traverser == null) {
            throw new RuntimeException("Error, traverser is null");
        }
        return traverser;
    }

    /////////////////////////////////////////////////////////////////////
    // Encode
    /////////////////////////////////////////////////////////////////////

    public Row encode(final Traverser traverser) {
        final List<Object> objects = new ArrayList<>();
        for (final TraverserEncoder encoder : orderedTraverserEncoders) {
            encoder.encode(objects, traverser);
        }
        return RowFactory.create(objects.toArray(new Object[0]));
    }

    public Row encode(final Element element, final TraversalMatrix tm, final String step) {
        final List<Object> objects = new ArrayList<>();
        for (final ElementEncoder encoder : orderedElementEncoders) {
            encoder.encode(objects, tm, element, step);
        }
        return RowFactory.create(objects.toArray(new Object[0]));
    }

    /////////////////////////////////////////////////////////////////////
    // Requirements
    /////////////////////////////////////////////////////////////////////

    abstract Set<TraverserRequirement> getRequirements();

    /////////////////////////////////////////////////////////////////////
    // Constants
    /////////////////////////////////////////////////////////////////////

    public enum TRAVERSER_TYPE {
        VERTEX,
        EDGE,
        VERTEX_PROPERTY,
        META_PROPERTY,
        EDGE_PROPERTY
    }

    public enum ID_TYPE {
        STRING,
        INTEGER,
        LONG
    }

    public enum CodecRequirements {
        BASE,
        BULK,
        SINGLE_LOOP,
        PATH,
        NESTED_LOOP
    }

    /////////////////////////////////////////////////////////////////////
    // Utility
    /////////////////////////////////////////////////////////////////////
}
