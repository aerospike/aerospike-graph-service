package com.aerospike.firefly.structure.util;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.FireflyVertexProperty;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

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

    private static void readEdgesToList(FireflyGraph graph, Iterator<Long> edgeIds, List<Edge> edges, String[] edgeLabels) {
        while (edgeIds.hasNext()) {
            Optional.ofNullable(
                    graph.readEdge(FireflyId.of(FireflyEdge.class, edgeIds.next()))).ifPresent(edge -> {
                if (edgeLabels.length == 0 || Arrays.asList(edgeLabels).contains(edge.label())) {
                    edges.add(edge);
                }
            });
        }
    }

    public static Iterator<Edge> getEdges(FireflyGraph graph, FireflyVertex vertex, Direction direction, String[] edgeLabels) {
        final List<Edge> edges = new ArrayList<>();

        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            readEdgesToList(graph, vertex.getEdgeIdsFromVertex(Direction.OUT), edges, edgeLabels);
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            readEdgesToList(graph, vertex.getEdgeIdsFromVertex(Direction.IN), edges, edgeLabels);
        }
        return edges.iterator();
    }

    public static Iterator<Vertex> getVertices(FireflyGraph graph, FireflyVertex vertex, Direction direction, String[] edgeLabels) {
        AerospikeConnection db = ((FireflyGraph) vertex.graph()).getBaseGraph();
        final List<Vertex> vertices = new ArrayList<>();
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            if (vertex.getEdgeIdsFromVertex(Direction.OUT).hasNext()) {
                if (edgeLabels.length == 0) {
                    vertex.getEdgeIdsFromVertex(Direction.OUT).forEachRemaining(id ->
                            vertices.add(graph.readEdge(FireflyId.of(FireflyVertex.class, id)).inVertex()));
                } else {
                    vertex.getEdgeIdsFromVertex(Direction.OUT).forEachRemaining(id -> {
                        Edge e = graph.readEdge(FireflyId.of(FireflyVertex.class, id));
                        if (IteratorUtils.anyMatch(IteratorUtils.asIterator(edgeLabels), it -> it.equals(e.label())))
                            vertices.add(e.inVertex());
                    });
                }
            }
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            if (vertex.getEdgeIdsFromVertex(Direction.IN).hasNext()) {
                if (edgeLabels.length == 0) {
                    vertex.getEdgeIdsFromVertex(Direction.IN).forEachRemaining(id ->
                            vertices.add(graph.readEdge(FireflyId.of(FireflyVertex.class, id)).outVertex()));
                } else {
                    vertex.getEdgeIdsFromVertex(Direction.IN).forEachRemaining(id -> {
                        Edge e = graph.readEdge(FireflyId.of(FireflyEdge.class, id));
                        if (IteratorUtils.anyMatch(IteratorUtils.asIterator(edgeLabels), it -> it.equals(e.label())))
                            vertices.add(e.outVertex());
                    });
                }
            }
        }
        return vertices.iterator();
    }

    public static Iterator<FireflyEdge> queryEdgeStringIndex(FireflyGraph graph, String key, Object value) {
        return graph.queryEdgePropertyStringMatchIndex(key, value);
    }

    public static Iterator<? extends Edge> queryEdgeNumericIndex(FireflyGraph graph, String key, P<?> predicate) {
        if (predicate.getBiPredicate().equals(Compare.eq))
            return graph.queryEdgePropertyNumericMatchIndex(key, predicate);
        else if (predicate.getBiPredicate().equals(Compare.lt))
            return graph.queryEdgePropertyNumericRangeIndex(key, predicate);
        else if (predicate.getBiPredicate().equals(Compare.gt))
            return graph.queryEdgePropertyNumericRangeIndex(key, predicate);
        else
            throw new RuntimeException("Predicate not supported on index query " + predicate.getBiPredicate());
    }

    public static Iterator<? extends Vertex> queryVertexByLabelStringIndex(FireflyGraph graph, Object value) {
        return graph.queryVertexLabelStringIndex(value);
    }

    public static Iterator<? extends Edge> queryEdgeByLabelStringIndex(FireflyGraph graph, Object value) {
        return graph.queryEdgeLabelStringIndex(value);
    }


    public static Iterator<? extends Vertex> queryVertexByVertexPropertyStringIndex(FireflyGraph graph, String key, Object value) {
        return IteratorUtils.map(graph.queryVertexPropertyStringIndex(key, value), FireflyVertexProperty::element);
    }

    public static Iterator<? extends Vertex> queryVertexByVertexPropertyNumericIndex(FireflyGraph graph, String key, P<?> predicate) {
        if (predicate.getBiPredicate().equals(Compare.eq))
            return IteratorUtils.map(graph.queryVertexPropertyNumberMatchIndex(key, predicate), FireflyVertexProperty::element);
        else if (predicate.getBiPredicate().equals(Compare.lt))
            return IteratorUtils.map(graph.queryVertexPropertyNumberRangeIndex(key, predicate), FireflyVertexProperty::element);
        else if (predicate.getBiPredicate().equals(Compare.gt))
            return IteratorUtils.map(graph.queryVertexPropertyNumberRangeIndex(key, predicate), FireflyVertexProperty::element);
        else
            throw new RuntimeException("Predicate not supported on index query " + predicate.getBiPredicate());
    }

    public static long countVertices(FireflyGraph graph) {
        return graph.getVertexCount();
    }

    public static long countEdges(FireflyGraph graph) {
        return graph.getEdgeCount();
    }
}
