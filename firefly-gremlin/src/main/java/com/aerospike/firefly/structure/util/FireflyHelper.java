package com.aerospike.firefly.structure.util;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.*;

import static com.aerospike.firefly.io.AerospikeConnection.SupportedValueTypes;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public final class FireflyHelper {
    private FireflyHelper() {
    }

    public static boolean inComputerMode(final FireflyGraph graph) {
        return false;
    }

    public static FireflyVertex readVertex(FireflyGraph graph, FireflyId id) {
        return graph.getBaseGraph().readVertex(graph, id);
    }

    public static void writeVertex(FireflyGraph graph, FireflyId id, String label) {
        graph.getBaseGraph().writeVertex(graph, id, label);
    }

    public static void removeVertex(FireflyGraph graph, FireflyId id) {
        graph.getBaseGraph().removeVertex(graph, id);
    }


    public static Edge addEdge(final FireflyGraph graph, final FireflyVertex outVertex, final FireflyVertex inVertex, final String label, final Object... keyValues) {
        final FireflyId fid = FireflyId.createFromKeyValuesOrManager(graph, FireflyEdge.class, keyValues);
        graph.getBaseGraph().writeEdge(graph, fid, label, outVertex, inVertex, new Object[]{});
        FireflyEdge edge = new FireflyEdge(fid, label, outVertex.id, inVertex.id, graph);
        ElementHelper.attachProperties(edge, keyValues);
        return edge;
    }

    public static <V> V validateGraphVariableValue(V v) {
        Set<Class<? extends Serializable>> supported = SupportedValueTypes.keySet();
        if (v != null && !supported.contains(v.getClass()))
            throw Graph.Variables.Exceptions.dataTypeOfVariableValueNotSupported(v);
        return v;
    }

    public static <V> V validatePropertyValue(V v) {
        Set<Class<? extends Serializable>> supported = SupportedValueTypes.keySet();
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
        AerospikeConnection db = ((FireflyGraph) vertex.graph()).getBaseGraph();
        final List<Edge> edges = new ArrayList<>();

        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (((FireflyGraph) vertex.graph()).getBaseGraph().getOutEdgeIdsFromVertex(vertex).hasNext()) {
                if (edgeLabels.length == 0) {
                    ((FireflyGraph) vertex.graph()).getBaseGraph().getOutEdgeIdsFromVertex(vertex).forEachRemaining(id -> Optional.ofNullable(db.readEdge((FireflyGraph) vertex.graph(), FireflyId.of(((FireflyGraph) vertex.graph()).getBaseGraph(), FireflyEdge.class, id))).ifPresent(edges::add));
                } else {
                    ((FireflyGraph) vertex.graph()).getBaseGraph().getOutEdgeIdsFromVertex(vertex).forEachRemaining(id -> {
                        Optional<Edge> e = Optional.ofNullable(db.readEdge((FireflyGraph) vertex.graph(), FireflyId.of(((FireflyGraph) vertex.graph()).getBaseGraph(), FireflyEdge.class, id)));
                        e.ifPresent(edge -> IteratorUtils.asIterator(edgeLabels).forEachRemaining(label -> {
                            if (label.equals(edge.label()))
                                edges.add(edge);
                        }));
                    });
                }
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (((FireflyGraph) vertex.graph()).getBaseGraph().getInEdgeIdsFromVertex(vertex).hasNext()) {
                if (edgeLabels.length == 0) {
                    ((FireflyGraph) vertex.graph()).getBaseGraph().getInEdgeIdsFromVertex(vertex).forEachRemaining(id -> Optional.ofNullable(
                            db.readEdge((FireflyGraph) vertex.graph(), FireflyId.of(((FireflyGraph) vertex.graph()).getBaseGraph(), FireflyVertex.class, id))).ifPresent(edges::add));
                } else {
                    ((FireflyGraph) vertex.graph()).getBaseGraph().getInEdgeIdsFromVertex(vertex).forEachRemaining(id -> {
                        Optional<Edge> e = Optional.ofNullable(db.readEdge((FireflyGraph) vertex.graph(), FireflyId.of(((FireflyGraph) vertex.graph()).getBaseGraph(), FireflyVertex.class, id)));
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
        AerospikeConnection db = ((FireflyGraph) vertex.graph()).getBaseGraph();
        final List<Vertex> vertices = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (((FireflyGraph) vertex.graph()).getBaseGraph().getOutEdgeIdsFromVertex(vertex).hasNext()) {
                if (edgeLabels.length == 0) {
                    ((FireflyGraph) vertex.graph()).getBaseGraph().getOutEdgeIdsFromVertex(vertex).forEachRemaining(id -> {
                        vertices.add(db.readEdge((FireflyGraph) vertex.graph(), FireflyId.of(((FireflyGraph) vertex.graph()).getBaseGraph(), FireflyVertex.class, id)).inVertex());
                    });
                } else {
                    ((FireflyGraph) vertex.graph()).getBaseGraph().getOutEdgeIdsFromVertex(vertex).forEachRemaining(id -> {
                        Edge e = db.readEdge((FireflyGraph) vertex.graph(), FireflyId.of(((FireflyGraph) vertex.graph()).getBaseGraph(), FireflyVertex.class, id));
                        if (IteratorUtils.anyMatch(IteratorUtils.asIterator(edgeLabels), it -> it.equals(e.label())))
                            vertices.add(e.inVertex());
                    });
                }
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (((FireflyGraph) vertex.graph()).getBaseGraph().getInEdgeIdsFromVertex(vertex).hasNext()) {
                if (edgeLabels.length == 0) {
                    ((FireflyGraph) vertex.graph()).getBaseGraph().getInEdgeIdsFromVertex(vertex).forEachRemaining(id -> {
                        vertices.add(db.readEdge((FireflyGraph) vertex.graph(), FireflyId.of(((FireflyGraph) vertex.graph()).getBaseGraph(), FireflyVertex.class, id)).outVertex());
                    });
                } else {
                    ((FireflyGraph) vertex.graph()).getBaseGraph().getInEdgeIdsFromVertex(vertex).forEachRemaining(id -> {
                        Edge e = db.readEdge((FireflyGraph) vertex.graph(), FireflyId.of(((FireflyGraph) vertex.graph()).getBaseGraph(), FireflyEdge.class, id));
                        if (IteratorUtils.anyMatch(IteratorUtils.asIterator(edgeLabels), it -> it.equals(e.label())))
                            vertices.add(e.outVertex());
                    });
                }
            }
        }
        return vertices.iterator();
    }

    public static List<FireflyEdge>  queryEdgeIndex(FireflyGraph graph, String key, Object value) {
        return null;
    }

    public static List<FireflyVertex>  queryVertexIndex(FireflyGraph graph, String key, Object value) {
        return null;
    }

    public static long countVertices(FireflyGraph graph) {
        return graph.getBaseGraph().getVertexCount();
    }

    public static long countEdges(FireflyGraph graph) {
        return graph.getBaseGraph().getEdgeCount();
    }
}
