package com.aerospike.firefly.olap.codec;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceElement;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

import java.util.Set;

public abstract class RowCodec {

    /////////////////////////////////////////////////////////////////////
    // Columns
    /////////////////////////////////////////////////////////////////////
    public static final String TRAVERSER_TYPE_COL = "~traverser_type";
    public static final String ID_COL = "~id";
    public static final String ID_TYPEHINT_COL = "~id_typehint";
    public static final String LABEL_COL = "~label";
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


    /////////////////////////////////////////////////////////////////////
    // Schema
    /////////////////////////////////////////////////////////////////////

    protected StructType getBaseSchema() {
        return new StructType()
                .add(TRAVERSER_TYPE_COL, DataTypes.IntegerType, false)
                .add(ID_COL, DataTypes.StringType, false)
                .add(ID_TYPEHINT_COL, DataTypes.IntegerType, false)
                .add(LABEL_COL, DataTypes.StringType, false)
                .add(HALTED_COL, DataTypes.BooleanType, false)
                .add(STEP_COL, DataTypes.StringType, false);
    }

    abstract StructType getSchema();

    /////////////////////////////////////////////////////////////////////
    // Decode
    /////////////////////////////////////////////////////////////////////

    abstract Traverser decode(final Row row, final TraverserGenerator tg, final TraversalMatrix tm);

    /////////////////////////////////////////////////////////////////////
    // Encode
    /////////////////////////////////////////////////////////////////////

    abstract Row encode(final Traverser traverser);
    abstract Row encode(final Vertex vertex, final String step);
    abstract Row encode(final Edge edge, final String step);

    /////////////////////////////////////////////////////////////////////
    // Requirements
    /////////////////////////////////////////////////////////////////////

    abstract Set<TraverserRequirement> getRequirements();




    public static Object getId(final String id, final int idTypeOrdinal) {
        if (ID_TYPE.STRING.ordinal() == idTypeOrdinal) {
            return id;
        } else if (ID_TYPE.LONG.ordinal() == idTypeOrdinal) {
            return Long.parseLong(id);
        } else if (ID_TYPE.INTEGER.ordinal() == idTypeOrdinal) {
            return Integer.parseInt(id);
        } else {
            // TODO.
            throw new IllegalArgumentException("Only string int and long ids are supported in olap");
        }
    }

    /////////////////////////////////////////////////////////////////////
    // Constants
    /////////////////////////////////////////////////////////////////////

    public enum TRAVERSER_TYPE {
        VERTEX,
        EDGE
    }

    public enum ID_TYPE {
        STRING,
        INTEGER,
        LONG
    }

    /////////////////////////////////////////////////////////////////////
    // Utility
    /////////////////////////////////////////////////////////////////////

    public static ID_TYPE getIdType(final Object id) {
        if (id instanceof Long) {
            return ID_TYPE.LONG;
        } else if (id instanceof Integer) {
            return ID_TYPE.INTEGER;
        } else if (id instanceof  String) {
            return ID_TYPE.STRING;
        } else {
            // TODO.
            throw new IllegalArgumentException("Only long string and integer types can be serialized at this time " + id.getClass().getName() + " is not supported.");
        }
    }

    public static ReferenceElement getReferenceElement(final int elementTypeOrdinal, final Object id, final String label) {
        if (TRAVERSER_TYPE.VERTEX.ordinal() == elementTypeOrdinal) {
            return new ReferenceVertex(id);
        } else if (TRAVERSER_TYPE.EDGE.ordinal() == elementTypeOrdinal) {
            return new ReferenceEdge(id, null, null, null);
        } else {
            throw new RuntimeException("Error, decoder for " + elementTypeOrdinal + " is not implemented");
        }
    }
}
