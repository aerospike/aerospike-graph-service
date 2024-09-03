package com.aerospike.firefly.util;

import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.query.GraphQuery;
import com.aerospike.firefly.io.aerospike.query.paged.GraphQueryHelper;
import com.aerospike.firefly.process.computer.local.LocalGraphComputerView;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.iterator.FireflyBatchEdgeIterator;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.aerospike.firefly.io.aerospike.AerospikeConnection.SupportedValueTypes;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public final class FireflyHelper {
    static private final Logger LOG = LoggerFactory.getLogger(FireflyHelper.class);

    private FireflyHelper() {
    }

    //////////////////////////////////////////////////////////////////////////////////////////////////////////////
    //////////////////////////////////// FIREFLY GRAPH COMPUTER //////////////////////////////////////////////////
    //////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static boolean inComputerMode(final FireflyGraph graph) {
        return graph.graphComputerView != null;
    }

    public static LocalGraphComputerView createGraphComputerView(final FireflyGraph graph, final GraphFilter graphFilter, final Set<VertexComputeKey> computeKeys) {
        return graph.graphComputerView = new LocalGraphComputerView(graph, graphFilter, computeKeys);
    }

    public static void dropGraphComputerView(final FireflyGraph graph) {
        graph.graphComputerView= null;
    }

    public static LocalGraphComputerView getGraphComputerView(final FireflyGraph graph) {
        return  graph.graphComputerView;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

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
            if (key == null)
                throw Property.Exceptions.propertyKeyCanNotBeNull();
            if (String.class.equals(key.getClass())) {
                if (key.toString().isEmpty())
                    throw Property.Exceptions.propertyKeyCanNotBeEmpty();
            }

            i.next();
        }
    }

    public static Iterator<Edge> getEdges(FireflyGraph graph, FireflyVertex vertex, Direction direction, String[] edgeLabels) {
        final Set<String> labels = new HashSet<>(Arrays.asList(edgeLabels));
        return new FireflyBatchEdgeIterator<>(graph, vertex.getEdgeIdsFromVertex(direction, labels, Collections.emptyList()));
    }

    public static long countVertices(final FireflyGraph graph, final List<HasContainer> hasContainers, final Long evaluationTimeout) {
        final AerospikeConnection db = graph.getBaseGraph();
        if (hasContainers.isEmpty()) {
            return graph.getVertexCount(new ArrayList<>(), evaluationTimeout);
        }

        final Optional<FireflyIndexMetadata.IndexInfo> info = graph.fireflyIndexMetadata.getPropertyIndexInfo(
                FireflyVertex.class, hasContainers.get(0).getKey(), hasContainers.get(0).getValue());
        if (info.isPresent()) {
            final HasContainer topHasContainer = hasContainers.remove(0);

            // Create query policy with expressions.
            final QueryPolicy queryPolicy = new QueryPolicy();
            queryPolicy.filterExp = GraphQueryHelper.hasContainerListToExpression(db, hasContainers, FireflyVertex.class);
            queryPolicy.includeBinData = false;
            queryPolicy.setTimeout(evaluationTimeout.intValue());

            // Query index.
            final Iterator<KeyRecord> keyRecordIterator = GraphQuery.create(graph).querySIndex(
                    info.get().setName,
                    info.get().indexName,
                    GraphQueryHelper.predicateToFilter(db, topHasContainer.getPredicate(), info.get()),
                    queryPolicy);

            // Transform record to correct element.
            return FireflyCloseableIteratorUtils.count(keyRecordIterator);
        } else {
            // Get vertex count.
            return graph.getVertexCount(hasContainers, evaluationTimeout);
        }
    }

    public static long countEdges(final FireflyGraph graph, final Long evaluationTimeout) {
        return graph.getEdgeCount(evaluationTimeout);
    }
}
