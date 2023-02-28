package com.aerospike.firefly.structure.util;

import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aerospike.firefly.io.AerospikeConnection.SupportedValueTypes;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public final class FireflyHelper {
    static private final Logger LOG = LoggerFactory.getLogger(FireflyHelper.class);

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
        // TODO: This returns a raw list and could blow up on a supernode.
        return graph.readEdges(List.of(), vertex.getEdgeIdsFromVertex(direction)).stream().filter(
                        edge -> (labels.isEmpty() || labels.contains(edge.label()))).
                collect(Collectors.toList());
    }

    public static Iterator<Edge> getEdges(FireflyGraph graph, FireflyVertex vertex, Direction direction, String[] edgeLabels) {
        final Set<String> labels = new HashSet<>(Arrays.asList(edgeLabels));
        return getEdgeList(graph, vertex, direction, labels).iterator();
    }

    public static long countVertices(FireflyGraph graph) {
        return graph.getVertexCount();
    }

    public static long countEdges(FireflyGraph graph) {
        return graph.getEdgeCount();
    }
}
