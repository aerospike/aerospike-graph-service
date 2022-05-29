package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.*;

import static com.aerospike.firefly.io.AerospikeConnection.SupportedTypes;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyHelper {
    private FireflyHelper() {
    }

    public static boolean inComputerMode(final FireflyGraph graph) {
        return false;
    }

    protected static FireflyVertex readVertex(FireflyGraph graph, Object id) {
        return graph.db.readVertex(graph, id);
    }

    protected static void writeVertex(FireflyGraph graph, Object id, String label) {
        graph.db.writeVertex(graph, id, label);
    }

    public static void removeVertex(FireflyGraph graph, Object id) {
        graph.db.removeVertex(graph, id);
    }


    protected static Edge addEdge(final FireflyGraph graph, final FireflyVertex outVertex, final FireflyVertex inVertex, final String label, final Object... keyValues) {
        final Object id = ElementHelper.getIdValue(keyValues).orElse(graph.edgeIdManager.getNextId(graph));
        graph.db.writeEdge(graph, id, label, outVertex, inVertex, new Object[]{});
        FireflyEdge e = graph.db.readEdge(graph, id);
        ElementHelper.attachProperties(e,keyValues);
        return e;
    }

    public static <V> V validateGraphVariableValue(V v) {
        Set<Class<? extends Serializable>> supported = SupportedTypes.keySet();
        if (v != null && !supported.contains(v.getClass()))
            throw Graph.Variables.Exceptions.dataTypeOfVariableValueNotSupported(v);
        return v;
    }

    public static <V> V validatePropertyValue(V v) {
        Set<Class<? extends Serializable>> supported = SupportedTypes.keySet();
        if (v != null && !supported.contains(v.getClass()))
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(v);
        return v;
    }

    public static void legalPropertyKeyValueArray(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Iterator i = IteratorUtils.asIterator(keyValues);
        while (i.hasNext()) {
            Object key = i.next();
            if (String.class.equals(key.getClass())) {
                if (key.toString().isEmpty())
                    throw Property.Exceptions.propertyKeyCanNotBeEmpty();
                else if (key == null)
                    throw Property.Exceptions.propertyKeyCanNotBeNull();
            }

            i.next();
        }
    }

    public static Iterator<Edge> getEdges(FireflyVertex vertex, Direction direction, String[] edgeLabels) {
        AerospikeConnection db = ((FireflyGraph) vertex.graph()).db;
        final List<Edge> edges = new ArrayList<>();

        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (vertex.getOutEdgeIds().hasNext()) {
                if (edgeLabels.length == 0) {
                    vertex.getOutEdgeIds().forEachRemaining(id -> Optional.ofNullable(db.readEdge((FireflyGraph) vertex.graph(), id)).ifPresent(edges::add));
                }else {
                    vertex.getOutEdgeIds().forEachRemaining(id -> {
                        Optional<Edge> e = Optional.ofNullable(db.readEdge((FireflyGraph) vertex.graph(), id));
                        e.ifPresent(edge -> IteratorUtils.asIterator(edgeLabels).forEachRemaining(label -> {
                            if (label.equals(edge.label()))
                                edges.add(edge);
                        }));
                    });
                }
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (vertex.getInEdgeIds().hasNext()) {
                if (edgeLabels.length == 0) {
                    vertex.getInEdgeIds().forEachRemaining(id -> Optional.ofNullable(
                            db.readEdge((FireflyGraph) vertex.graph(), id)).ifPresent(edges::add));
                } else {
                    vertex.getInEdgeIds().forEachRemaining(id -> {
                        Optional<Edge> e = Optional.ofNullable(db.readEdge((FireflyGraph) vertex.graph(), id));
                        e.ifPresent(edge -> IteratorUtils.asIterator(edgeLabels).forEachRemaining(label -> {
                            if (label.equals(edge.label()))
                                edges.add(edge);
                        }));
                    });
                }
            }
        }
        return edges.iterator();
    }

    public static Iterator<Vertex> getVertices(FireflyVertex vertex, Direction direction, String[] edgeLabels) {
        AerospikeConnection db = ((FireflyGraph) vertex.graph()).db;
        final List<Vertex> vertices = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (vertex.getOutEdgeIds().hasNext()) {
                if (edgeLabels.length == 0) {
                    vertex.getOutEdgeIds().forEachRemaining(id -> {
                        vertices.add(db.readEdge((FireflyGraph) vertex.graph(), id).inVertex());
                    });
                } else {
                    vertex.getOutEdgeIds().forEachRemaining(id -> {
                        Edge e = db.readEdge((FireflyGraph) vertex.graph(), id);
                        if (IteratorUtils.anyMatch(IteratorUtils.asIterator(edgeLabels), it -> it.equals(e.label())))
                            vertices.add(e.inVertex());
                    });
                }
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (vertex.getInEdgeIds().hasNext()) {
                if (edgeLabels.length == 0) {
                    vertex.getInEdgeIds().forEachRemaining(id -> {
                        vertices.add(db.readEdge((FireflyGraph) vertex.graph(), id).outVertex());
                    });
                } else {
                    vertex.getInEdgeIds().forEachRemaining(id -> {
                        Edge e = db.readEdge((FireflyGraph) vertex.graph(), id);
                        if (IteratorUtils.anyMatch(IteratorUtils.asIterator(edgeLabels), it -> it.equals(e.label())))
                            vertices.add(e.outVertex());
                    });
                }
            }
        }
        return vertices.iterator();
    }
}
