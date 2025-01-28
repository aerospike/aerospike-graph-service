package com.aerospike.firefly.olap.structure;

import com.aerospike.firefly.structure.FireflyVertex;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.aerospike.firefly.olap.structure.DistributedGraphComputer.getIdType;

public class DistributedCodec {

    static final String ID_COL = "~id";
    static final String ID_TYPEHINT_COL = "~id_typehint";
    static final String LABEL_COL = "~label";
    static final String PROPERTIES_COL = "~properties";
    static final String IN_COL = "~in";
    static final String OUT_COL = "~out";
    static final String HALTED_COL = "~halted";
    static final String REF_COL = "~ref";
    static final String STEP_COL = "~step";
    static final String BULK_COL = "~bulk";

    public static StructType schema() {
        return new StructType()
                .add(ID_COL, DataTypes.StringType, false)
                .add(ID_TYPEHINT_COL, DataTypes.IntegerType, false)
                .add(LABEL_COL, DataTypes.StringType, true)
                .add(HALTED_COL, DataTypes.BooleanType, false)
                .add(STEP_COL, DataTypes.StringType, false)
                .add(BULK_COL, DataTypes.LongType, false);
    }

    public static Row encode(final Traverser traverser) {
        if (traverser.get() instanceof Vertex) {
            final Vertex vertex = (Vertex) traverser.get();
            return RowFactory.create(vertex.id().toString(), // String id.
                    getIdType(vertex.id()).ordinal(), // Integer ordinal.
                    vertex.label(), // String label.
                    traverser.asAdmin().isHalted(), // Boolean halted.
                    traverser.asAdmin().getStepId(), // String step.
                    traverser.bulk()); // Integer bulk.
        } else {
            throw new RuntimeException("Error, encoder for " + traverser.get().getClass() + " is not implemented");
        }
    }

    public static Row encode(final Vertex vertex, final String step) {
        return RowFactory.create(vertex.id().toString(), getIdType(vertex.id()).ordinal(), vertex.label(), false, step, 1L);
    }

    public static Traverser decode(final Row row,
                                   final TraverserGenerator traverserGenerator,
                                   final TraversalMatrix traversalMatrix) {
        if (row.getString(row.fieldIndex(ID_COL)) == null) {
            throw new RuntimeException("Error, only rows with id col populated are currently supported");
        }
        final Object id = getId(row.getString(row.fieldIndex(ID_COL)), row.getInt(row.fieldIndex(ID_TYPEHINT_COL)));
        final String label = row.getString(row.fieldIndex(LABEL_COL));
        final String step = row.getString(row.fieldIndex(STEP_COL));
        final long bulk = row.getLong(row.fieldIndex(BULK_COL));
        final ReferenceVertex vertex = new ReferenceVertex(id, label);
        final Traverser traverser = traverserGenerator.generate(vertex, traversalMatrix.getStepById(step), bulk);
        traverser.asAdmin().setStepId(step);
        return traverser;
    }

    private static Object getId(final String id, final int idTypeOrdinal) {
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

    enum ID_TYPE {
        STRING,
        INTEGER,
        LONG
    }

    // TODO: Leave for now maybe remove later. This stuff was annoying to figure out and if we need to do it later I dont wanna rewrite it..
    private Row createRow(final DistributedVertex vertex) {
        return RowFactory.create(
                vertex.id,
                vertex.idTypeOrdinal,
                vertex.label(),
                vertex.getScalaProperties(),
                vertex.getScalaEdges(Direction.IN),
                vertex.getScalaEdges(Direction.OUT));
    }


    public static Row createRow(final FireflyVertex vertex, Boolean halted) {
        //final TraverserGenerator generator = traversal.asAdmin().getTraverserGenerator();
        // TODO: Make a better format, stringifying these is going to be slow.
        scala.collection.mutable.Map<String, String> properties = JavaConverters.mapAsScalaMap(vertex.getRawVertexStringPropertyValues());
        final Map<String, List<byte[]>> inEdgesJava = vertex.getCachedIdMap(Direction.IN);
        final Map<String, Seq<byte[]>> inEdgesScala = new HashMap<>();
        for (final String label : inEdgesJava.keySet()) {
            inEdgesScala.put(label, JavaConverters.asScalaBuffer(inEdgesJava.get(label)));
        }
        final Map<String, List<byte[]>> outEdgesJava = vertex.getCachedIdMap(Direction.OUT);
        final Map<String, Seq<byte[]>> outEdgesScala = new HashMap<>();
        for (final String label : outEdgesJava.keySet()) {
            outEdgesScala.put(label, JavaConverters.asScalaBuffer(outEdgesJava.get(label)));
        }
        scala.collection.mutable.Map<String, Seq<byte[]>> inEdges = JavaConverters.mapAsScalaMap(inEdgesScala);
        scala.collection.mutable.Map<String, Seq<byte[]>> outEdges = JavaConverters.mapAsScalaMap(outEdgesScala);

        return RowFactory.create(
                vertex.id().toString(),
                getIdType(vertex.id()).ordinal(),
                vertex.label(),
                properties,
                inEdges,
                outEdges,
                halted);
    }

    public static <A, B> scala.collection.mutable.Map<A, B> toScalaMap(HashMap<A, B> m) {
        return JavaConverters.mapAsScalaMapConverter(m).asScala();
    }
}
