package com.aerospike.firefly.io.aerospike.admin.services;

import com.aerospike.client.AerospikeException;
import com.aerospike.client.ResultCode;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.Task;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIteratorUtils;
import com.aerospike.firefly.util.WarmupUtil;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.aerospike.firefly.util.ConfigurationHelper.Keys.Bins.LABEL_BIN;
import static com.aerospike.firefly.util.ConfigurationHelper.Keys.InternalConfigs.V_LABEL_INDEX_NAME;

public class AdminSindexServicePlugin<I, R> extends AdminServiceFactoryBase<I, R> {
    final static String LIST = "list";
    final static String CARDINALITY = "cardinality";
    final static String CREATE_VERTEX_PROPERTY = "create-vertex-property";
    final static String CREATE_VERTEX_LABEL = "create-vertex-label";
    final static String GET_STATUS = "get-status";

    final static Set<String> PROPERTY_KEYS = Set.of(LIST, CARDINALITY, CREATE_VERTEX_PROPERTY, CREATE_VERTEX_LABEL, GET_STATUS);

    public AdminSindexServicePlugin(final FireflyGraph firefly) {
        super(firefly);
    }

    @Override
    protected String adminServiceName() {
        return "sindex";
    }

    @Override
    protected Map<String, String> getParamDescription() {
        return Map.of(
                LIST, "Get list of existing indexes. Poll this to determine when an index is ready for use.",
                CARDINALITY, "Get map of existing indexes to their cardinality.",
                CREATE_VERTEX_PROPERTY, "Create a secondary index on vertex properties (vertex property key supplied as value).",
                GET_STATUS, "Get the status of an index creation operation.",
                CREATE_VERTEX_LABEL, "Create a secondary index on all vertex labels."
        );
    }

    private String usage(Map params) {
        return String.format(
                "The following is acceptable usage:\n" +
                        "\t'%s' (no value and no other parameters),\n" +
                        "\t'%s' (no value and no other parameters),\n" +
                        "\t'%s' (String value required, add '%s' after to get status),\n" +
                        "\t'%s' (String value required, add '%s' after to get status).\n + " +
                        "\tProvided: %s",
                LIST, CARDINALITY, CREATE_VERTEX_PROPERTY, GET_STATUS, CREATE_VERTEX_LABEL, GET_STATUS, params.toString());
    }

    public void sanitize(final Map params) {
        if ((params == null || params.isEmpty()) ||
                (!params.containsKey(GET_STATUS) && params.size() > 1) ||
                (params.containsKey(GET_STATUS) && params.size() != 2))  {
            throw new IllegalArgumentException(usage(params));
        }
        if (params.containsKey(LIST)) {
            if (params.get(LIST) != null) {
                throw new IllegalArgumentException(usage(params));
            }
        } else if (params.containsKey(CARDINALITY)) {
            if (params.get(CARDINALITY) != null) {
                throw new IllegalArgumentException(usage(params));
            }
        } else if (params.containsKey(CREATE_VERTEX_PROPERTY)) {
            if (params.get(CREATE_VERTEX_PROPERTY) == null) {
                throw new IllegalArgumentException(usage(params));
            } else if (!(params.get(CREATE_VERTEX_PROPERTY) instanceof String)) {
                throw new IllegalArgumentException(usage(params));
            }
        } else if (params.containsKey(CREATE_VERTEX_LABEL)) {
            if (params.get(CREATE_VERTEX_LABEL) == null) {
                throw new IllegalArgumentException(usage(params));
            }
        } else {
            throw new IllegalArgumentException(usage(params));
        }
    }

    @Override
    public CloseableIterator<R> execute(final ServiceCallContext ctx, final Map params) {
        sanitize(params);
        if (params.containsKey(LIST)) {
            return listIndexes();
        } else if (params.containsKey(CARDINALITY)) {
            return indexCardinality();
        } else if (params.containsKey(CREATE_VERTEX_PROPERTY)) {
            return createVertexPropertyIndex((String) params.get(CREATE_VERTEX_PROPERTY));
        } else if (params.containsKey(CREATE_VERTEX_LABEL)) {
            return createVertexLabelIndex();
        } else {
            // Technically covered by sanitize but not having this feels incomplete.
            throw new IllegalArgumentException(usage(params));
        }
    }

    private CloseableIterator<R> listIndexes() {
        try {
            // Manually force an update.
            firefly.fireflyCardinalityMetadata.updateMetadata();
        } catch (Exception e) {
            return FireflyCloseableIteratorUtils.of((R) ("Failed to update index information. " + e.getMessage()));
        }

        final List<String> vertexPropertyIndexes = firefly.fireflyCardinalityMetadata.getVertexPropertyIndexes();
        final boolean vertexLabelIndex = firefly.fireflyCardinalityMetadata.getVertexLabelIndexExists();
        if (vertexLabelIndex) {
            vertexPropertyIndexes.add("~label");
        }

        // Return as a list so it can be used programatically.
        return FireflyCloseableIteratorUtils.of((R) vertexPropertyIndexes);
    }

    private CloseableIterator<R> indexCardinality() {
        try {
            // Manually force an update.
            firefly.fireflyCardinalityMetadata.updateMetadata();
        } catch (Exception e) {
            return FireflyCloseableIteratorUtils.of((R) "Failed to update index information. " + e.getMessage());
        }

        final List<String> vertexPropertyIndexes = firefly.fireflyCardinalityMetadata.getVertexPropertyIndexes();
        final boolean vertexLabelIndex = firefly.fireflyCardinalityMetadata.getVertexLabelIndexExists();
        final Map<String, Long> cardinalityMap = new HashMap<>();
        if (vertexLabelIndex) {
            firefly.fireflyCardinalityMetadata.getVertexLabelCardinality().ifPresent(cardinality -> {
                Long cardinalityValue = cardinality.getCardinality();
                if (cardinalityValue != null) {
                    cardinalityMap.put("~label", cardinalityValue);
                }
            });
        }
        vertexPropertyIndexes.forEach(index -> {
            try {
                cardinality.put(index, info.recordCount);
            } catch (AerospikeException e) {
                if (e.getResultCode() == ResultCode.INDEX_NOTFOUND) {
                    cardinality.put(index, 0L);
                } else {
                    throw e;
                }
            }
        });
    }

    private CloseableIterator<R> createVertexPropertyIndex(final String key) {
        final String set = firefly.getBaseGraph().setFromElementType(FireflyVertex.class);
        final List<String> existingIndexes = getExistingIndexes();
        try {
            firefly.getBaseGraph().createIndexBackground(existingIndexes,
                    set,
                    firefly.getBaseGraph().V_LABEL_INDEX_NAME,
                    firefly.getBaseGraph().LABEL_BIN,
                    key,
                    IndexType.STRING,
                    IndexCollectionType.DEFAULT);
        } catch (RuntimeException e) {
            // Note this is something like: "Index __ already exists".
            return FireflyCloseableIteratorUtils.of((R) e.getMessage());
        }
        return FireflyCloseableIteratorUtils.of((R) "Vertex label sindex creation in progress.");
    }

    private List<String> getExistingIndexes() {
        return AerospikeConnection.InfoOps.
                listExistingIndexes(firefly.getBaseGraph().getClient(), firefly.getBaseGraph().getNamespace()).
                stream().map(Map.Entry::getKey).collect(Collectors.toList());
    }

    private CloseableIterator<R> createVertexLabelIndex() {
        final String set = firefly.getBaseGraph().setFromElementType(FireflyVertex.class);
        final List<String> existingIndexes = getExistingIndexes();
        try {
            firefly.getBaseGraph().createIndexBackground(existingIndexes,
                    set,
                    firefly.getBaseGraph().V_LABEL_INDEX_NAME,
                    firefly.getBaseGraph().LABEL_BIN,
                    null, // keyName is null for label, as this is the key for properties.
                    IndexType.STRING,
                    IndexCollectionType.DEFAULT);
        } catch (RuntimeException e) {
            // Note this is something like: "Index __ already exists".
            return FireflyCloseableIteratorUtils.of((R) e.getMessage());
        }
        return FireflyCloseableIteratorUtils.of((R) "Vertex label sindex creation in progress.");
    }
}
