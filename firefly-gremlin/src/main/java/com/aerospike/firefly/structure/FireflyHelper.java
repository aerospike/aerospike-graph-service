package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

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
        //user supplied ids not supported
        if (ElementHelper.getIdValue(keyValues).isPresent())
            throw new UnsupportedOperationException();
        Object id = graph.edgeIdManager.getNextId(graph);
        graph.db.writeEdge(graph, id, label, outVertex, inVertex, keyValues);
        return graph.db.readEdge(graph, id);
    }

    public static <V> V validatePropertyValue(V v) {
        List<Class<? extends Serializable>> supported = List.of(String.class, Long.class, Boolean.class, Double.class, byte[].class);
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
                assert !key.toString().isEmpty();
            }

            i.next();
        }
    }

    public static Iterator<Edge> getEdges(FireflyVertex vertex, Direction direction, String[] edgeLabels) {
        AerospikeConnection db = ((FireflyGraph) vertex.graph()).db;
        final List<Edge> edges = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (!vertex.getOutEdgeIds().isEmpty()) {
                if (edgeLabels.length == 0) {
                    vertex.getOutEdgeIds().forEach(id -> Optional.ofNullable(
                            db.readEdge((FireflyGraph) vertex.graph(), id)).ifPresent(edges::add));
                } else {
                    vertex.getOutEdgeIds().forEach(id -> {
                        Optional<Edge> e = Optional.ofNullable(db.readEdge((FireflyGraph) vertex.graph(), id));
                        if (e.isPresent() && IteratorUtils.anyMatch(IteratorUtils.asIterator(edgeLabels), it -> it == e.get().label()))
                            e.ifPresent(edges::add);
                    });
                }
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (vertex.getInEdgeIds().hasNext()) {
                if (edgeLabels.length == 0) {
                    vertex.getInEdgeIds().forEachRemaining(id -> Optional.ofNullable(db.readEdge((FireflyGraph) vertex.graph(), id)).ifPresent(edges::add));
                } else {
                    vertex.getInEdgeIds().forEachRemaining(id -> {
                        Optional<Edge> e = Optional.ofNullable(db.readEdge((FireflyGraph) vertex.graph(), id));
                        if (e.isPresent() && IteratorUtils.anyMatch(IteratorUtils.asIterator(edgeLabels), it -> it == e.get().label()))
                            e.ifPresent(edges::add);
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
            if (!vertex.getOutEdgeIds().isEmpty()) {
                if (edgeLabels.length == 0) {
                    vertex.getOutEdgeIds().forEach(id -> {
                        vertices.add(db.readEdge((FireflyGraph) vertex.graph(), id).inVertex());
                    });
                } else {
                    vertex.getOutEdgeIds().forEach(id -> {
                        Edge e = db.readEdge((FireflyGraph) vertex.graph(), id);
                        if (IteratorUtils.anyMatch(IteratorUtils.asIterator(edgeLabels), it -> it == e.label()))
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
                        if (IteratorUtils.anyMatch(IteratorUtils.asIterator(edgeLabels), it -> it == e.label()))
                            vertices.add(e.outVertex());
                    });
                }
            }
        }
        return vertices.iterator();
    }
}
