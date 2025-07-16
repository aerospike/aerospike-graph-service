package com.aerospike.firefly.util;

import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.query.paged.VertexQueryHelper;
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
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.nio.ByteBuffer;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aerospike.firefly.io.aerospike.AerospikeConnection.AEROSPIKE_TRANSFORMABLE_TYPES;
import static com.aerospike.firefly.io.aerospike.AerospikeConnection.SUPPORTED_ARR_TYPES;
import static com.aerospike.firefly.io.aerospike.AerospikeConnection.SUPPORTED_VALUE_TYPES;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public final class FireflyHelper {
    static private final byte[] TRUE_BOOL_BYTES = new byte[]{1};
    static private final byte[] FALSE_BOOL_BYTES = new byte[]{0};

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
        graph.graphComputerView = null;
    }

    public static LocalGraphComputerView getGraphComputerView(final FireflyGraph graph) {
        return graph.graphComputerView;
    }

    ///////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    public static Object validateGraphVariableValue(final Object v) {
        return validatePropertyValue(v);
    }

    public static Object validatePropertyValue(final Object v) {
        final Object value;
        if (v != null && SUPPORTED_ARR_TYPES.contains(v.getClass())) {
            final ArrayList vList = new ArrayList<>();
            if (v instanceof boolean[]) {
                final boolean[] vArray = (boolean[]) v;
                for (int i = 0; i < vArray.length; i++) {
                    vList.add(vArray[i]);
                }
            } else if (v instanceof double[]) {
                final double[] vArray = (double[]) v;
                for (int i = 0; i < vArray.length; i++) {
                    vList.add(vArray[i]);
                }
            } else if (v instanceof int[]) {
                final int[] vArray = (int[]) v;
                for (int i = 0; i < vArray.length; i++) {
                    vList.add(vArray[i]);
                }
            } else if (v instanceof long[]) {
                final long[] vArray = (long[]) v;
                for (int i = 0; i < vArray.length; i++) {
                    vList.add(vArray[i]);
                }
            } else if (v instanceof Date[]) {
                final Date[] vArray = (Date[]) v;
                for (int i = 0; i < vArray.length; i++) {
                    vList.add(vArray[i]);
                }
            } else if (v instanceof OffsetDateTime[]) {
                final OffsetDateTime[] vArray = (OffsetDateTime[]) v;
                for (int i = 0; i < vArray.length; i++) {
                    vList.add(vArray[i]);
                }
            } else {
                final Object[] vArray = (Object[]) v;
                for (int i = 0; i < vArray.length; i++) {
                    vList.add(vArray[i]);
                }
            }
            value = vList;
        } else {
            value = v;
        }
        if (value != null && !SUPPORTED_VALUE_TYPES.containsKey(value.getClass())) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(v);
        }
        return value;
    }

    public static Object validateAndConvertVertexPropertyValue(final Object v) {
        if (v instanceof List) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(v);
        }
        Object validatedValue = validatePropertyValue(v);
        if (validatedValue instanceof Boolean) {
            validatedValue = (Boolean) validatedValue ? TRUE_BOOL_BYTES : FALSE_BOOL_BYTES;
        } else if (validatedValue instanceof Double) {
            validatedValue = ByteBuffer.allocate(Double.BYTES).putDouble((Double) validatedValue).array();
        } else {
            validatedValue = convertValueToAerospikeWriteable(validatedValue);
        }
        return validatedValue;
    }

    public static void legalPropertyKeyValueArray(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        Arrays.stream(keyValues).collect(Collectors.toList()).iterator();
        Iterator<Object> i = Arrays.stream(keyValues).collect(Collectors.toList()).iterator();
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
            queryPolicy.filterExp = VertexQueryHelper.hasContainerListToExpression(db, hasContainers);
            queryPolicy.includeBinData = false;
            queryPolicy.setTimeout(evaluationTimeout.intValue());

            // Query index.
            final Iterator<KeyRecord> keyRecordIterator = graph.graphQuery.querySIndex(
                    info.get().setName,
                    info.get().indexName,
                    VertexQueryHelper.predicateToFilter(db, topHasContainer.getPredicate(), info.get()),
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

    // Cast value type to Aerospike supported type if necessary (matching type hint will be added later).
    public static Object convertValueToAerospikeWriteable(final Object value) {
        if (value instanceof List) {
            final List<?> originalList = (List<?>) value;
            final List<Object> transformedList = new ArrayList<>(originalList.size());
            for (final Object element : originalList) {
                if (element != null && AEROSPIKE_TRANSFORMABLE_TYPES.contains(element.getClass())) {
                    transformedList.add(FireflyHelper.typeCastPropertyValue(element));
                } else {
                    transformedList.add(element);
                }
            }
            return transformedList;
        }
        return (value != null && AEROSPIKE_TRANSFORMABLE_TYPES.contains(value.getClass()))
                ? FireflyHelper.typeCastPropertyValue(value)
                : value;
    }

    public static Object typeCastPropertyValue(Object val) {
        if (val instanceof Integer) {
            return ((Integer) val).longValue();
        }
        if (val instanceof Date) {
            return ((Date) val).getTime();
        }
        if (val instanceof OffsetDateTime) {
            return ((OffsetDateTime) val).toInstant().toEpochMilli();
        }
        throw new IllegalArgumentException("Could not cast given type to long.");
    }
}
