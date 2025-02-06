package com.aerospike.firefly.olap.codec;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ImmutablePath;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_NL_O_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_NL_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_LP_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_NL_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_NL_O_OB_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_NL_O_OB_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_P_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.LP_O_OB_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.NL_O_OB_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.O_OB_S_SE_SL_Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.LabelledCounter;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceEdge;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceElement;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import scala.collection.JavaConverters;
import scala.collection.Seq;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

import static com.aerospike.firefly.olap.codec.RowCodec.HALTED_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.ID_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.ID_TYPEHINT_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.LABEL_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.NL_COUNT_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.NL_NAME_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.NL_STEP_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.PATH_ID_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.PATH_ID_TYPEHINT_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.PATH_LABELS_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.PATH_OBJ_TYPE_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.SL_COUNT_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.SL_NAME_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.STEP_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.TRAVERSER_TYPE_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.ELEMENT_ID_COL;
import static com.aerospike.firefly.olap.codec.RowCodec.ELEMENT_ID_TYPEHINT_COL;

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

    public static class NestedLoopInfo {
        public final List<String> loopNames;
        public final List<String> loopSteps;
        public final List<Integer> loopCounts;

        public NestedLoopInfo(final List<String> loopNames, final List<Integer> loopCounts, final List<String> loopSteps) {
            this.loopNames = loopNames;
            this.loopCounts = loopCounts;
            this.loopSteps = loopSteps;
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
            if (info.pathLabels != null) {
                for (final Seq<String> labels : info.pathLabels) {
                    t.asAdmin().addLabels(new HashSet<>(JavaConverters.seqAsJavaList(labels)));
                }
            }
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
                int pathType = info.pathObjTypes.get(i);
                Object value;
                if (pathType == RowCodec.TRAVERSER_TYPE.EDGE.ordinal() ||pathType == RowCodec.TRAVERSER_TYPE.VERTEX_PROPERTY.ordinal() ||
                        pathType == RowCodec.TRAVERSER_TYPE.VERTEX.ordinal()) {
                    value = getReferenceElement(info.pathObjTypes.get(i), getId(info.pathIds.get(i), info.pathIdTypeHints.get(i)), null);
                } else if (pathType == RowCodec.TRAVERSER_TYPE.INTEGER.ordinal() || pathType == RowCodec.TRAVERSER_TYPE.STRING.ordinal()) {
                    value = getId(info.pathIds.get(i), info.pathIdTypeHints.get(i));
                } else {
                    throw new RuntimeException("Error, path type '" + pathType + "' is not supported.");
                }
                final Set<String> labels = new HashSet<>(JavaConverters.seqAsJavaListConverter(info.pathLabels.get(i)).asJava());
                path = path.extend(value, labels);
                pathField.set(t, path);
            }
        } catch (NoSuchFieldException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void setNestedLoops(final Traverser t, final NestedLoopInfo nli) {
        if (nli.loopNames == null) {
            return;
        }
        if (nli.loopCounts.isEmpty() || nli.loopNames.isEmpty() || nli.loopSteps.isEmpty()) {
            return;
        }
        for (int i = nli.loopCounts.size() - 1; i >= 0; i--) {
            final String loopStep = nli.loopSteps.get(i);
            final String loopName = nli.loopNames.get(i);
            t.asAdmin().initialiseLoops(loopName, loopStep.equals("~empty_loop") ? null : loopStep);
            for (int j = 0; j < nli.loopCounts.get(i); j++) {
                t.asAdmin().incrLoops();
            }
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

    public static NestedLoopInfo getNestedLoopInfo(final Traverser t) {
        final Class baseClass;
        if (t instanceof NL_O_OB_S_SE_SL_Traverser) {
            baseClass = NL_O_OB_S_SE_SL_Traverser.class;
        } else if (t instanceof LP_NL_O_OB_S_SE_SL_Traverser) {
            baseClass = LP_NL_O_OB_S_SE_SL_Traverser.class;
        } else if (t instanceof LP_NL_O_OB_P_S_SE_SL_Traverser) {
            baseClass = LP_NL_O_OB_P_S_SE_SL_Traverser.class;
        } else if (t instanceof B_NL_O_S_SE_SL_Traverser) {
            baseClass = B_NL_O_S_SE_SL_Traverser.class;
        } else if (t instanceof B_LP_NL_O_S_SE_SL_Traverser) {
            baseClass = B_LP_NL_O_S_SE_SL_Traverser.class;
        } else if (t instanceof B_LP_NL_O_P_S_SE_SL_Traverser) {
            baseClass = B_LP_NL_O_P_S_SE_SL_Traverser.class;
        } else {
            throw new RuntimeException("Error, traverser type '" + t.getClass() + "' does not support nested loops.");
        }
        try {
            final Field nestedLoopsField = baseClass.getDeclaredField("nestedLoops");
            nestedLoopsField.setAccessible(true);
            final Stack<LabelledCounter> labelledCounters = (Stack<LabelledCounter>) nestedLoopsField.get(t);
            final List<String> loopNames = new ArrayList<>();
            final List<Integer> loopCounts = new ArrayList<>();
            List<String> loopSteps = new ArrayList<>();
            final Field loopNamesField = baseClass.getDeclaredField("loopNames");
            loopNamesField.setAccessible(true);
            for (LabelledCounter lc : labelledCounters) {
                final Field labelField = LabelledCounter.class.getDeclaredField("label");
                labelField.setAccessible(true);
                final String label = (String) labelField.get(lc);
                loopNames.add(label);
                loopCounts.add(lc.count());
            }
            final org.apache.commons.collections.map.ReferenceMap loopNamesMap = (org.apache.commons.collections.map.ReferenceMap) loopNamesField.get(t);
            for (final String loopName : loopNames) {
                final Iterator<Object> it = loopNamesMap.entrySet().iterator();
                if (it.hasNext()) {
                    while (it.hasNext()) {
                        Map.Entry<String, LabelledCounter> entry = (Map.Entry<String, LabelledCounter>) it.next();
                        LabelledCounter lc = (LabelledCounter) entry.getValue();
                        final Field loopLabel = LabelledCounter.class.getDeclaredField("label");
                        loopLabel.setAccessible(true);
                        final String subLoopName = (String) loopLabel.get(lc);
                        if (loopName.equals(subLoopName)) {
                            loopSteps.add(entry.getKey());
                        }
                    }
                } else {
                    loopSteps.add("~empty_loop");
                }
            }
            return new NestedLoopInfo(loopNames, loopCounts, loopSteps);
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
                } else if (e instanceof VertexProperty) {
                    objTypes.add(RowCodec.TRAVERSER_TYPE.VERTEX_PROPERTY.ordinal());
                }
            } else if (o instanceof Integer) {
                ids.add(o.toString());
                idTypeHints.add(RowCodec.ID_TYPE.INTEGER.ordinal());
                objTypes.add(RowCodec.TRAVERSER_TYPE.INTEGER.ordinal());;
            } else if (o instanceof String) {
                ids.add(o.toString());
                idTypeHints.add(RowCodec.ID_TYPE.STRING.ordinal());
                objTypes.add(RowCodec.TRAVERSER_TYPE.STRING.ordinal());
            } else {
                throw new RuntimeException("Error, only elements are currently supported '" + o.getClass() + "' is not supported.");
            }
        }
        for (final Set<String> labels : path.labels()) {
            labelsList.add(JavaConverters.asScalaBufferConverter(new ArrayList<>(labels)).asScala());
        }
        return new PathInfo(ids, idTypeHints, objTypes, labelsList);
    }

    public static void addBaseRow(final List<Object> row, TraversalMatrix tm, final Element element, final String step) {
        if (element instanceof Vertex) {
            row.add(RowCodec.TRAVERSER_TYPE.VERTEX.ordinal()); // Integer traverser type.
            row.add(null);
            row.add(null);
        } else if (element instanceof Edge) {
            row.add(RowCodec.TRAVERSER_TYPE.EDGE.ordinal()); // Integer traverser type.
            row.add(null);
            row.add(null);
        } else if (element instanceof VertexProperty) {
            row.add(RowCodec.TRAVERSER_TYPE.VERTEX_PROPERTY.ordinal()); // Integer traverser type.
            final Vertex v = ((VertexProperty<?>) element).element();
            row.add(v.id().toString());
            row.add(getIdType(v.id()).ordinal());
        } else {
            throw new RuntimeException("Error, encoder for " + element.getClass() + " is not implemented");
        }
        row.add(element.id().toString()); // String id.
        row.add(getIdType(element.id()).ordinal()); // Integer ordinal.
        row.add(element.label()); // String label.
        row.add(tm.getStepById(step) instanceof EmptyStep || tm.getStepById(step) == null); // Boolean halted.
        row.add(step); // String step.
    }

    public static void addBaseRow(final List<Object> row, final Traverser traverser) {
        final Object t = traverser.get();
        if (t instanceof Element) {
            final Element element = (Element) t;
            if (element instanceof Vertex) {
                row.add(RowCodec.TRAVERSER_TYPE.VERTEX.ordinal()); // Integer traverser type.
                row.add(null);
                row.add(null);
            } else if (element instanceof Edge) {
                row.add(RowCodec.TRAVERSER_TYPE.EDGE.ordinal()); // Integer traverser type.
                row.add(null);
                row.add(null);
            } else if (element instanceof VertexProperty) {
                row.add(RowCodec.TRAVERSER_TYPE.VERTEX_PROPERTY.ordinal()); // Integer traverser type.
                final Vertex v = ((VertexProperty<?>) element).element();
                row.add(v.id().toString());
                row.add(getIdType(v.id()).ordinal());
            }
            row.add(element.id().toString()); // String id.
            row.add(getIdType(element.id()).ordinal()); // Integer ordinal.
            row.add(element.label()); // String label.
            row.add(traverser.asAdmin().isHalted()); // Boolean halted. TODO: Is this always OK? What about g.V()?
            row.add(traverser.asAdmin().getStepId()); // String step.
        } else if (t instanceof Property) {
            final Property property = (Property) t;
            final Element e = property.element();
            if (e instanceof VertexProperty) {
                throw new RuntimeException("Vertex meta properties are not implemented at this time. Please contact support.");
                //final Vertex v = ((VertexProperty<?>) e).element();
                //row.add(e.id().toString());
                //row.add(getIdType(e.id()).ordinal());
                //row.add(RowCodec.TRAVERSER_TYPE.META_PROPERTY.ordinal()); // Integer traverser type.
            } else {
                row.add(RowCodec.TRAVERSER_TYPE.EDGE_PROPERTY.ordinal()); // Integer traverser type.
                row.add(e.id().toString());
                row.add(getIdType(e.id()).ordinal());
            }
            row.add(property.key()); // String id.
            row.add(getIdType(property.key()).ordinal()); // Integer ordinal.
            row.add(null); // String label.
            row.add(traverser.asAdmin().isHalted()); // Boolean halted. TODO: Is this always OK? What about g.V()?
            row.add(traverser.asAdmin().getStepId()); // String step.
        }else {
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

    public static void addNestedLoops(List<Object> row, final NestedLoopInfo nli) {
        row.add(JavaConverters.asScalaBufferConverter(nli.loopCounts).asScala().toSeq()); // Array of Integer loop counts.
        row.add(JavaConverters.asScalaBufferConverter(nli.loopNames).asScala().toSeq()); // Array of String loop names.
        row.add(JavaConverters.asScalaBufferConverter(nli.loopSteps).asScala().toSeq()); // Array of String loop steps.
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
            return new ReferenceEdge(id, "~empty", new ReferenceVertex("~empty"), new ReferenceVertex("~empty"));
        } else {
            throw new RuntimeException("Error, decoder for " + elementTypeOrdinal + " is not implemented for path references.");
        }
    }

    public static StructType appendPathSchema(StructType schema) {
        return schema
                .add(PATH_ID_COL, DataTypes.createArrayType(DataTypes.StringType), true)
                .add(PATH_ID_TYPEHINT_COL, DataTypes.createArrayType(DataTypes.IntegerType), true)
                .add(PATH_OBJ_TYPE_COL, DataTypes.createArrayType(DataTypes.IntegerType), true)
                .add(PATH_LABELS_COL, DataTypes.createArrayType(DataTypes.createArrayType(DataTypes.StringType)), true);
    }

    public static StructType appendNestedLoopSchema(StructType schema) {
        return schema
                .add(NL_COUNT_COL, DataTypes.createArrayType(DataTypes.IntegerType), true)
                .add(NL_NAME_COL, DataTypes.createArrayType(DataTypes.StringType), true)
                .add(NL_STEP_COL, DataTypes.createArrayType(DataTypes.StringType), true);
    }

    public static StructType appendSingleLoopSchema(StructType schema) {
        return schema
                .add(SL_COUNT_COL, DataTypes.IntegerType, true)
                .add(SL_NAME_COL, DataTypes.StringType, true);
    }

    public static StructType getBaseSchema() {
        return new StructType()
                .add(TRAVERSER_TYPE_COL, DataTypes.IntegerType, false)
                .add(ELEMENT_ID_COL, DataTypes.StringType, true)
                .add(ELEMENT_ID_TYPEHINT_COL, DataTypes.IntegerType, true)
                .add(ID_COL, DataTypes.StringType, false)
                .add(ID_TYPEHINT_COL, DataTypes.IntegerType, false)
                .add(LABEL_COL, DataTypes.StringType, true)
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
