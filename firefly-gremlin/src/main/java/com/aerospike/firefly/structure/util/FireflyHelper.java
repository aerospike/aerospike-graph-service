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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

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

    private static List<Edge> getEdgeList(final FireflyGraph graph, final FireflyVertex vertex, final Direction direction, final Set<String> labels) {
        return graph.readEdges(vertex.getEdgeIdsFromVertex(direction).
                                       stream().
                                       map(id -> FireflyId.of(FireflyEdge.class, id)).
                                       collect(Collectors.toList())).stream().filter(
                        edge -> (labels.isEmpty() || labels.contains(edge.label()))).
                collect(Collectors.toList());
    }

    public static Iterator<Edge> getEdges(FireflyGraph graph, FireflyVertex vertex, Direction direction, String[] edgeLabels) {
        final Set<String> labels = new HashSet<>(Arrays.asList(edgeLabels));
        return getEdgeList(graph, vertex, direction, labels).iterator();
    }

    public static Iterator<Vertex> getVertices(FireflyGraph graph, FireflyVertex vertex, Direction direction, String[] edgeLabels) {
        final List<Vertex> vertices = new ArrayList<>();
        final Set<String> labels = new HashSet<>(Arrays.asList(edgeLabels));
        if (direction.equals(Direction.OUT) || direction.equals(Direction.BOTH)) {
            final List<FireflyId> vertexIds = new ArrayList<>();
            for (final Edge edge : getEdgeList(graph, vertex, Direction.OUT, labels)) {
                vertexIds.add(((FireflyEdge)edge).inVertexId());
            }
            vertices.addAll(graph.readVertices(vertexIds));
        }
        if (direction.equals(Direction.IN) || direction.equals(Direction.BOTH)) {
            final List<FireflyId> vertexIds = new ArrayList<>();
            for (final Edge edge : getEdgeList(graph, vertex, Direction.IN, labels)) {
                vertexIds.add(((FireflyEdge)edge).outVertexId());
            }
            vertices.addAll(graph.readVertices(vertexIds));
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
