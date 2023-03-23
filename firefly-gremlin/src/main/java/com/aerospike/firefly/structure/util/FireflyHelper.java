package com.aerospike.firefly.structure.util;

import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
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
        Iterator i = FireflyCloseableIteratorUtils.asIterator(keyValues);
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

    public static long countVertices(final FireflyGraph graph, final List<HasContainer> hasContainers) {
        if (hasContainers.isEmpty()) {
            return graph.getVertexCount(null);
        }

        final Optional<FireflyIndexMetadata.IndexInfo> info = graph.fireflyIndexMetadata.getPropertyIndexInfo(
                FireflyVertex.class, hasContainers.get(0).getKey(), hasContainers.get(0).getValue());
        if (info.isPresent()) {
            final HasContainer topHasContainer = hasContainers.remove(0);

            // Create query policy with expressions.
            final QueryPolicy queryPolicy = new QueryPolicy();
            queryPolicy.filterExp = graph.hasContainerListToExpression(hasContainers, FireflyVertex.class);
            queryPolicy.includeBinData = false;

            // Query index.
            final Iterator<KeyRecord> keyRecordIterator = graph.getBaseGraph().queryIndex(
                    info.get().setName,
                    info.get().indexName,
                    graph.predicateToFilter(topHasContainer.getPredicate(), info.get()),
                    queryPolicy);

            // Transform record to correct element.
            return FireflyCloseableIteratorUtils.count(keyRecordIterator);
        } else {
            // Get vertex count.
            return graph.getVertexCount(graph.hasContainerListToExpression(hasContainers, FireflyVertex.class));
        }
    }

    public static long countEdges(FireflyGraph graph) {
        return graph.getEdgeCount();
    }
}
