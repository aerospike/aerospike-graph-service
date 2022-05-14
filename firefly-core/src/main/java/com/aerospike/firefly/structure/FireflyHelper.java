package com.aerospike.firefly.structure;

import com.aerospike.firefly.io.AerospikeConnection;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyHelper {
    public static boolean inComputerMode(final FireflyGraph graph) {
        return false;
    }

    protected static Edge addEdge(final FireflyGraph graph, final FireflyVertex outVertex, final FireflyVertex inVertex, final String label, final Object... keyValues) {
        Object id = graph.edgeIdManager.getNextId(graph);
        graph.db.writeEdge(graph, id, label, outVertex, inVertex, keyValues);
        return graph.db.readEdge(graph, id);
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
                        if (e.isPresent() && Arrays.stream(edgeLabels).collect(Collectors.toList()).contains(e.get().label()))
                            e.ifPresent(edges::add);
                    });
                }
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (!vertex.getInEdgeIds().isEmpty()) {
                if (edgeLabels.length == 0) {
                    vertex.getInEdgeIds().forEach(id -> Optional.ofNullable(db.readEdge((FireflyGraph) vertex.graph(), id)).ifPresent(edges::add));
                } else {
                    vertex.getInEdgeIds().forEach(id -> {
                        Optional<Edge> e = Optional.ofNullable(db.readEdge((FireflyGraph) vertex.graph(), id));
                        if (e.isPresent() && Arrays.stream(edgeLabels).collect(Collectors.toList()).contains(e.get().label()))
                            e.ifPresent(edges::add);
                    });
                }
            }
        }
        return (Iterator) edges.iterator();
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
                        if (Arrays.stream(edgeLabels).collect(Collectors.toList()).contains(e.label()))
                            vertices.add(e.inVertex());
                    });
                }
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (!vertex.getInEdgeIds().isEmpty()) {
                if (edgeLabels.length == 0) {
                    vertex.getInEdgeIds().forEach(id -> {
                        vertices.add(db.readEdge((FireflyGraph) vertex.graph(), id).outVertex());
                    });
                } else {
                    vertex.getInEdgeIds().forEach(id -> {
                        Edge e = db.readEdge((FireflyGraph) vertex.graph(), id);
                        if (Arrays.stream(edgeLabels).collect(Collectors.toList()).contains(e.label()))
                            vertices.add(e.outVertex());
                    });
                }
            }
        }
        return (Iterator) vertices.iterator();
    }
}
