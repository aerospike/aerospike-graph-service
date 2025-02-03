package com.aerospike.firefly.olap.codec;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.O_OB_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceElement;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.aerospike.firefly.olap.codec.RowCodec.HALTED_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.ID_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.ID_TYPEHINT_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.LABEL_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.PATH_ID_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.PATH_ID_TYPEHINT_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.PATH_LABELS_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.PATH_OBJ_TYPE_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.SL_COUNT_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.SL_NAME_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.STEP_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.TRAVERSER_TYPE_COL;

public class RowCodecHelper {

    public static class PathInfo {
        private final List<String> pathIds;
        private final List<Integer> pathIdTypeHints;
        private final List<Integer> pathObjTypes;
        private final List<Seq<String>> pathLabels;

        public PathInfo(final List<String> pathIds,
                        final List<Integer> pathIdTypeHints,
                        final List<Integer> pathObjTypes,
                        final List<Seq<String>> pathLabels) {
            this.pathIds = pathIds;
            this.pathIdTypeHints = pathIdTypeHints;
            this.pathObjTypes = pathObjTypes;
            this.pathLabels = pathLabels;
        }
    }

    public static class SingleLoopInfo {
        public final int loopCount;
        public final String loopName;

        public SingleLoopInfo(final int loopCount, final String loopName) {
            this.loopCount = loopCount;
            this.loopName = loopName;
        }
    }

    public static void setPath(final Traverser t, final PathInfo info) {
        if (info.pathIds == null) {
            return;
        }
        try {
            Class clazz;
            if (t instanceof LP_O_OB_P_S_SE_SL_Traverser) {
                clazz = LP_O_OB_P_S_SE_SL_Traverser.class;
            } else if (t instanceof B_LP_O_P_S_SE_SL_Traverser) {
                clazz = B_LP_O_P_S_SE_SL_Traverser.class;
            } else if (t instanceof B_LP_O_S_SE_SL_Traverser) {
                clazz = B_LP_O_S_SE_SL_Traverser.class;
            } else if (t instanceof LP_O_OB_S_SE_SL_Traverser) {
                clazz = LP_O_OB_S_SE_SL_Traverser.class;
            } else {
                throw new RuntimeException("Error, traverser type '" + t.getClass() + "' does not support paths.");
            }
            Field pathField = clazz.getDeclaredField("path");
            pathField.setAccessible(true);
            Path path = ImmutablePath.make();
            pathField.set(t, path);
            for (int i = 0; i < info.pathIds.size(); i++) {
                final Element e = getReferenceElement(info.pathObjTypes.get(i), getId(info.pathIds.get(i), info.pathIdTypeHints.get(i)), null);
                final Set<String> labels = new HashSet<>(JavaConverters.seqAsJavaListConverter(info.pathLabels.get(i)).asJava());
                path = path.extend(e, labels);
                pathField.set(t, path);
            }
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static SingleLoopInfo getSingleLoopInfo(final Traverser t) {
        final Class baseClass = t instanceof B_O_Traverser ? B_O_S_SE_SL_Traverser.class : O_OB_S_SE_SL_Traverser.class;
        try {
            Field loopNameField = baseClass.getDeclaredField("loopName");
            loopNameField.setAccessible(true);
            Field loopsField = baseClass.getDeclaredField("loops");
            loopsField.setAccessible(true);
            return new SingleLoopInfo(loopsField.getInt(t), (String) loopNameField.get(t));
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static PathInfo getPathInfo(final Traverser t) {
        final Path path = t.path();
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
                    objTypes.add(RowCodec.TRAVERSER_TYPE.VERTEX.ordinal());
                } else if (e instanceof Edge) {
                    objTypes.add(RowCodec.TRAVERSER_TYPE.EDGE.ordinal());
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
        return new PathInfo(ids, idTypeHints, objTypes, labelsList);
    }

    public static void addBaseRow(final List<Object> row, final Element element, final String step) {
        row.add(element instanceof Vertex ? RowCodec.TRAVERSER_TYPE.VERTEX.ordinal() : RowCodec.TRAVERSER_TYPE.EDGE.ordinal()); // Integer traverser type.
        row.add(element.id().toString()); // String id.
        row.add(getIdType(element.id()).ordinal()); // Integer ordinal.
        row.add(element.label()); // String label.
        row.add(false); // Boolean halted. TODO: Is this always OK? What about g.V()?
        row.add(step); // String step.
    }

    public static void addBaseRow(final List<Object> row, final Traverser traverser) {
        final Object t = traverser.get();
        if (t instanceof Element) {
            final Element element = (Element) t;
            row.add(element instanceof Vertex ? RowCodec.TRAVERSER_TYPE.VERTEX.ordinal() : RowCodec.TRAVERSER_TYPE.EDGE.ordinal()); // Integer traverser type.
            row.add(element.id().toString()); // String id.
            row.add(getIdType(element.id()).ordinal()); // Integer ordinal.
            row.add(element.label()); // String label.
            row.add(traverser.asAdmin().isHalted()); // Boolean halted. TODO: Is this always OK? What about g.V()?
            row.add(traverser.asAdmin().getStepId()); // String step.
        } else {
            throw new RuntimeException("Error, encoder for " + t.getClass() + " is not implemented");
        }
    }

    public static void addBulk(final List<Object> row, final Traverser traverser) {
        row.add(traverser.bulk()); // Integer bulk.
    }

    public static void addSingleLoop(final List<Object> row , final SingleLoopInfo sli) {
        row.add(sli.loopCount); // Loop count for single loop.
        row.add(sli.loopName);// Loop name for single loop.
    }

    public static void addPath(final List<Object> row, final PathInfo pi) {
        row.add(JavaConverters.asScalaBufferConverter(pi.pathIds).asScala().toSeq()); // Array of String ids.
        row.add(JavaConverters.asScalaBufferConverter(pi.pathIdTypeHints).asScala().toSeq()); // Array of Integer id type hints.
        row.add(JavaConverters.asScalaBufferConverter(pi.pathObjTypes).asScala().toSeq()); // Array of Integer object types.
        row.add(JavaConverters.asScalaBufferConverter(pi.pathLabels).asScala().toSeq()); // Array of Array of String labels.
    }

    public static RowCodec.ID_TYPE getIdType(final Object id) {
        if (id instanceof Long) {
            return RowCodec.ID_TYPE.LONG;
        } else if (id instanceof Integer) {
            return RowCodec.ID_TYPE.INTEGER;
        } else if (id instanceof  String) {
            return RowCodec.ID_TYPE.STRING;
        } else {
            // TODO.
            throw new IllegalArgumentException("Only long string and integer types can be serialized at this time " + id.getClass().getName() + " is not supported.");
        }
    }

    public static ReferenceElement getReferenceElement(final int elementTypeOrdinal, final Object id, final String label) {
        if (RowCodec.TRAVERSER_TYPE.VERTEX.ordinal() == elementTypeOrdinal) {
            return new ReferenceVertex(id);
        } else if (RowCodec.TRAVERSER_TYPE.EDGE.ordinal() == elementTypeOrdinal) {
            return new ReferenceEdge(id, null, null, null);
        } else {
            throw new RuntimeException("Error, decoder for " + elementTypeOrdinal + " is not implemented");
        }
    }

    public static StructType appendPathSchema(StructType schema) {
        return schema
                .add(PATH_ID_COL, DataTypes.createArrayType(DataTypes.StringType), true)
                .add(PATH_ID_TYPEHINT_COL, DataTypes.createArrayType(DataTypes.IntegerType), true)
                .add(PATH_OBJ_TYPE_COL, DataTypes.createArrayType(DataTypes.IntegerType), true)
                .add(PATH_LABELS_COL, DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.StringType)), true);
    }

    public static StructType appendSingleLoopSchema(StructType schema) {
        return schema
                .add(SL_COUNT_COL, DataTypes.IntegerType, true)
                .add(SL_NAME_COL, DataTypes.StringType, true);
    }

    public static StructType getBaseSchema() {
        return new StructType()
                .add(TRAVERSER_TYPE_COL, DataTypes.IntegerType, false)
                .add(ID_COL, DataTypes.StringType, false)
                .add(ID_TYPEHINT_COL, DataTypes.IntegerType, false)
                .add(LABEL_COL, DataTypes.StringType, false)
                .add(HALTED_COL, DataTypes.BooleanType, false)
                .add(STEP_COL, DataTypes.StringType, false);
    }


    public static Object getId(final String id, final int idTypeOrdinal) {
        if (RowCodec.ID_TYPE.STRING.ordinal() == idTypeOrdinal) {
            return id;
        } else if (RowCodec.ID_TYPE.LONG.ordinal() == idTypeOrdinal) {
            return Long.parseLong(id);
        } else if (RowCodec.ID_TYPE.INTEGER.ordinal() == idTypeOrdinal) {
            return Integer.parseInt(id);
        } else {
            // TODO.
            throw new IllegalArgumentException("Only string int and long ids are supported in olap");
        }
    }
}
