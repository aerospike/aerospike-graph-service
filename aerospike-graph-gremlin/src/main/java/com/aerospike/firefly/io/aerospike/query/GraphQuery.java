package com.aerospike.firefly.io.aerospike.query;

import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.query.legacy.LegacyGraphQuery;
import com.aerospike.firefly.io.aerospike.query.paged.GraphQueryHelper;
import com.aerospike.firefly.io.aerospike.query.paged.PageFetcher;
import com.aerospike.firefly.io.aerospike.query.paged.PagedGraphQuery;
import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIterator;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface GraphQuery {
    FireflyGraph getGraph();

    public static final Logger LOG = LoggerFactory.getLogger(GraphQuery.class);
    public static final String PAGED_MESSAGE = "using paged query implementation";
    public static final String LEGACY_MESSAGE = "using legacy query implementation";

    static GraphQuery create(FireflyGraph fireflyGraph) {
        if (fireflyGraph.getBaseGraph().QUERY_IMPL.equals(ConfigurationHelper.Keys.QUERY_PAGED)) {
            LOG.debug(PAGED_MESSAGE);
            return new PagedGraphQuery(fireflyGraph);
        } else if (fireflyGraph.getBaseGraph().QUERY_IMPL.equals(ConfigurationHelper.Keys.QUERY_LEGACY)) {
            LOG.debug(LEGACY_MESSAGE);
            return new LegacyGraphQuery(fireflyGraph);
        } else
            throw new RuntimeException("unknown query impl: " + fireflyGraph.getBaseGraph().QUERY_IMPL);
    }

    default Iterator<FireflyId> scanVertexIds(final Long evaluationTimeout) {
        return scanElementIds(FireflyVertex.class, List.of(), evaluationTimeout);
    }

    default BlockingQueue<PageFetcher.Page> scanVertexIdPages(final List<HasContainer> hasContainers,
                                                              final Long evaluationTimeout) {
        final P<?> predicate;
        final String binName;
        final String mapKey;
        final FireflyGraph graph = getGraph();
        final AerospikeConnection db = graph.getBaseGraph();
        if (!hasContainers.isEmpty()) {
            final HasContainer container = hasContainers.remove(0);
            predicate = container.getPredicate();
            if ("~label".equals(container.getKey())) {
                binName = graph.getBaseGraph().LABEL_BIN;
                mapKey = null;
            } else {
                binName = graph.getBaseGraph().VERTEX_PROPERTY_NAME_TO_VALUE_BIN;
                mapKey = container.getKey();
            }
        } else {
            predicate = null;
            binName = null;
            mapKey = null;
        }

        return scanSetPagesBlocking(mapKey, db.VERTEX_AERO_SET, binName, predicate, graph::vertexFromRecord,
                hasContainers, FireflyVertex.class, true, true, evaluationTimeout);
    }

    default Iterator<FireflyId> scanEdgeIds(final Long evaluationTimeout) {
        return scanElementIds(FireflyEdge.class, List.of(), evaluationTimeout);
    }

    default BlockingQueue<PageFetcher.Page> partitionVertexIdPages(final List<HasContainer> hasContainers, final Long evaluationTimeout) {
        P<?> predicate = null;
        String binName = null;
        String mapKey = null;
        final FireflyGraph graph = getGraph();
        final AerospikeConnection db = graph.getBaseGraph();

        final List<HasContainer> positiveFilters = hasContainers.stream().filter(it -> !it.getBiPredicate().equals(Contains.without)).collect(Collectors.toList());
        if (!positiveFilters.isEmpty()) {
            final List<Object> ids = positiveFilters
                    .stream()
                    .filter(it -> "~id".equals(it.getKey()))
                    .map(HasContainer::getValue)
                    .flatMap(it -> it instanceof List ? ((List<?>) it).stream() : Stream.of(it))
                    .collect(Collectors.toList());
            final List<HasContainer> nonIdContainers = hasContainers.stream().filter(it -> !"~id".equals(it.getKey())).collect(Collectors.toList());

            // If there are id has containers, we can do a batch read.
            if (!ids.isEmpty()) {
                final List<FireflyGraphStep.HasContainerWithCardinality> sortedHasContainers = FireflyBatchReadHelper.getHasContainersWithCardinalityOrder(graph, FireflyVertex.class, nonIdContainers);
                final List<HasContainer> aerospikeSideHasContainers = FireflyBatchReadHelper.getAerospikeHasContainers(sortedHasContainers);
                final Expression expression = GraphQueryHelper.hasContainerListToExpression(db, aerospikeSideHasContainers, FireflyVertex.class);
                final BatchPolicy policy = new BatchPolicy();
                policy.setTimeout(evaluationTimeout.intValue());
                return batchReadVertexPagesBlocking(graph, expression, graph::vertexFromRecord, ids, evaluationTimeout);
            }

            final List<FireflyGraphStep.HasContainerWithCardinality> sortedHasContainers = FireflyBatchReadHelper.getHasContainersWithCardinalityOrder(graph, FireflyVertex.class, hasContainers);
            final List<HasContainer> aerospikeSideHasContainers = FireflyBatchReadHelper.getAerospikeHasContainers(sortedHasContainers);
            final HasContainer topContainer = aerospikeSideHasContainers.isEmpty() ? null : aerospikeSideHasContainers.remove(0);

            if (topContainer != null) {
                // Find index.
                final Optional<FireflyIndexMetadata.IndexInfo> propertyIndexInfo =
                        graph.fireflyIndexMetadata.getPropertyIndexInfo(FireflyVertex.class, topContainer.getKey(), topContainer.getValue());
                if (propertyIndexInfo.isPresent()) {
                    final QueryPolicy policy = new QueryPolicy();
                    policy.setTimeout(evaluationTimeout.intValue());
                    policy.setSocketTimeout(evaluationTimeout.intValue() / 3);
                    policy.setTotalTimeout(evaluationTimeout.intValue());
                    policy.totalTimeout = evaluationTimeout.intValue();
                    policy.socketTimeout = evaluationTimeout.intValue() / 3;

                    // Need to wrap with has container check
                    // If we have a property index, we can use it and read sindex pages.
                    return indexSetPagesBlocking(db.VERTEX_AERO_SET, propertyIndexInfo.get().indexName,
                            GraphQueryHelper.predicateToFilter(db, topContainer.getPredicate(), propertyIndexInfo.get()),
                            policy, graph::vertexIdFromRecord);
                }

                predicate = topContainer.getPredicate();
                if ("~label".equals(topContainer.getKey())) {
                    binName = graph.getBaseGraph().LABEL_BIN;
                } else {
                    binName = graph.getBaseGraph().VERTEX_PROPERTY_NAME_TO_VALUE_BIN;
                    mapKey = topContainer.getKey();
                }
            }
        }

        // Default to scan.
        return scanSetPagesBlocking(mapKey, db.VERTEX_AERO_SET, binName, predicate, graph::vertexIdFromRecord,
                hasContainers, FireflyVertex.class, true, true, evaluationTimeout);
    }

    default Iterator<FireflyId> scanVertexIds(List<HasContainer> hasContainers, Long evaluationTimeout) {
        return scanElementIds(FireflyVertex.class, hasContainers, evaluationTimeout);
    }

    default Iterator<FireflyId> scanElementIds(Class<? extends FireflyElement> clazz, List<HasContainer> hasContainers,
                                               Long evaluationTimeout) {
        final P<?> predicate;
        final String binName;
        final String mapKey;
        final FireflyGraph graph = getGraph();
        final AerospikeConnection db = graph.getBaseGraph();
        if (!hasContainers.isEmpty()) {
            if (clazz.isAssignableFrom(FireflyEdge.class)) {
                throw new IllegalArgumentException("Cannot push predicates down to edges.");
            }

            final HasContainer container = hasContainers.remove(0);
            predicate = container.getPredicate();
            if ("~label".equals(container.getKey())) {
                binName = graph.getBaseGraph().LABEL_BIN;
                mapKey = null;
            } else {
                binName = graph.getBaseGraph().VERTEX_PROPERTY_NAME_TO_VALUE_BIN;
                mapKey = container.getKey();
            }
        } else {
            predicate = null;
            binName = null;
            mapKey = null;
        }

        if (FireflyVertex.class.isAssignableFrom(clazz)) {
            return scanSet(mapKey, db.VERTEX_AERO_SET, binName, predicate, graph::vertexIdFromRecord,
                    hasContainers, clazz, true, true, evaluationTimeout);
        } else if (FireflyEdge.class.isAssignableFrom(clazz)) {
            return new FireflyPhatEdgeIdIterator(scanSet(
                    mapKey, db.EDGE_AERO_SET, binName, predicate, (it) -> it,
                    hasContainers, clazz, true, true, evaluationTimeout), db);
        } else {
            throw new IllegalArgumentException("Cannot scan all element ids for unknown class: " + clazz);
        }

    }

    default <E> Iterator<E> scanSet(String mapKey, String setName, String binName, P<?> predicate,
                                    FireflyGraph.TransformKeyRecord<E> transform, Long evaluationTimeout) {
        return scanSet(mapKey, setName, binName, predicate, transform, List.of(), FireflyVertex.class,
                true, true, evaluationTimeout);
    }

    <E> Iterator<E> scanSet(String mapKey, String setName, String binName, P<?> predicate,
                            FireflyGraph.TransformKeyRecord<E> transform, List<HasContainer> hasContainers,
                            Class<? extends FireflyElement> clazz, boolean sendKey, boolean includeBinData,
                            Long evaluationTimeout, String... binNames);

    <E> BlockingQueue<PageFetcher.Page> indexSetPagesBlocking(String setName,
                                                              String indexName,
                                                              Filter filter,
                                                              QueryPolicy policy,
                                                              FireflyGraph.TransformKeyRecord<E> transformKeyRecord);

    <E> BlockingQueue<PageFetcher.Page> scanSetPagesBlocking(String mapKey,
                                                             String setName,
                                                             String binName,
                                                             P<?> predicate,
                                                             FireflyGraph.TransformKeyRecord<E> transform,
                                                             List<HasContainer> hasContainers,
                                                             Class<? extends FireflyElement> clazz,
                                                             boolean sendKey,
                                                             boolean includeBinData,
                                                             Long evaluationTimeout,
                                                             String... binNames);

    <E> BlockingQueue<PageFetcher.Page> batchReadVertexPagesBlocking(FireflyGraph graph,
                                                                     Expression expression,
                                                                     FireflyGraph.TransformKeyRecord<E> transformKeyRecord,
                                                                     List<Object> idsToRead,
                                                                     Long evaluationTimeout);

    default <E> Iterator<E> queryVertexSIndex(FireflyIndexMetadata.IndexInfo indexInfo,
                                              P<?> predicate,
                                              FireflyGraph.TransformKeyRecord<E> transform,
                                              Long evaluationTimeout) {
        return queryVertexSIndex(indexInfo, predicate, transform, Collections.emptyList(), evaluationTimeout);
    }

    default <E> Iterator<E> queryVertexSIndex(FireflyIndexMetadata.IndexInfo indexInfo,
                                              P<?> predicate,
                                              FireflyGraph.TransformKeyRecord<E> transform,
                                              List<HasContainer> hasContainers,
                                              Long evaluationTimeout) {
        // Create query policy with expressions.
        final QueryPolicy queryPolicy = new QueryPolicy();
        getGraph().getBaseGraph().configureIndexPolicy(queryPolicy);
        queryPolicy.filterExp = GraphQueryHelper.hasContainerListToExpression(getGraph().getBaseGraph(), hasContainers, FireflyVertex.class);

        // Override default with evaluationTimeout if < default.
        if (evaluationTimeout != null) {
            if (evaluationTimeout < queryPolicy.totalTimeout) {
                queryPolicy.totalTimeout = evaluationTimeout.intValue();
            }
            if (evaluationTimeout < queryPolicy.socketTimeout) {
                queryPolicy.socketTimeout = evaluationTimeout.intValue();
            }
        }

        // Query index.
        return querySIndex(indexInfo.setName, indexInfo.indexName,
                GraphQueryHelper.predicateToFilter(getGraph().getBaseGraph(), predicate, indexInfo), queryPolicy, transform);
    }

    default <E> Iterator<E> querySIndex(String setName,
                                        String indexName,
                                        Filter filter,
                                        QueryPolicy policy) {
        return (Iterator<E>) querySIndex(setName, indexName, filter, policy, (it) -> it);
    }

    <E> Iterator<E> querySIndex(String setName, String indexName, Filter filter, QueryPolicy policy,
                                FireflyGraph.TransformKeyRecord<E> transformKeyRecord);
}
