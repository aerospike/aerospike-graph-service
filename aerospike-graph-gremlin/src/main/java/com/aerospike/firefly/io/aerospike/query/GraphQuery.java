package com.aerospike.firefly.io.aerospike.query;

import com.aerospike.client.Key;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.query.paged.*;
import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIterator;
import com.aerospike.firefly.util.ConfigurationHelper;
import com.google.common.collect.Lists;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.aerospike.firefly.io.FireflyRecord.getKey;

public class GraphQuery {
    final FireflyGraph graph;
    final AerospikeConnection db;

    public GraphQuery(final FireflyGraph graph) {
        this.graph = graph;
        this.db = graph.getBaseGraph();
    }

    ///////////////////////
    // Id scanning methods.
    ///////////////////////

    public Iterator<FireflyId> scanVertexIds(final Long evaluationTimeout) {
        return scanElementIds(FireflyVertex.class, List.of(), evaluationTimeout);
    }

    public Iterator<FireflyId> scanVertexIds(final List<HasContainer> hasContainers, final
    Long evaluationTimeout) {
        return scanElementIds(FireflyVertex.class, hasContainers, evaluationTimeout);
    }

    public Iterator<FireflyId> scanEdgeIds(final Long evaluationTimeout) {
        return scanElementIds(FireflyEdge.class, List.of(), evaluationTimeout);
    }

    private Iterator<FireflyId> scanElementIds(final Class<? extends FireflyElement> clazz,
                                              final List<HasContainer> hasContainers,
                                              final Long evaluationTimeout) {
        final P<?> predicate;
        final String binName;
        final String mapKey;
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

    ////////////////////////////
    // Element scanning methods.
    ////////////////////////////

    public <E> Iterator<E> scanSet(final String mapKey,
                                   final String setName,
                                   final String binName,
                                   final P<?> predicate,
                                   final FireflyGraph.TransformKeyRecord<E> transform,
                                   final Long evaluationTimeout) {
        return scanSet(mapKey, setName, binName, predicate, transform, List.of(), FireflyVertex.class,
                true, true, evaluationTimeout);
    }

    public <E> Iterator<E> scanSet(final String mapKey,
                                   final String setName,
                                   final String binName,
                                   final P<?> predicate,
                                   final FireflyGraph.TransformKeyRecord<E> transform,
                                   final List<HasContainer> hasContainers,
                                   final Class<? extends FireflyElement> clazz,
                                   final boolean sendKey,
                                   final boolean includeBinData,
                                   final Long evaluationTimeout,
                                   final String... binNames) {
        final ScanPolicy policy = new ScanPolicy();
        policy.sendKey = sendKey;
        policy.includeBinData = includeBinData;
        policy.setTimeout(evaluationTimeout.intValue());
        // Build expression using predicate.
        if (predicate != null) {
            final Exp exp = GraphQueryHelper.predicateToExpression(db, binName, mapKey, predicate);
            if (hasContainers.isEmpty() && !db.TTL_ENABLED_FLAG) {
                policy.filterExp = Exp.build(exp);
            } else {
                final Exp[] exps = GraphQueryHelper.hasContainerListToExpArray(db, hasContainers, clazz);
                final Exp[] allExps = new Exp[exps.length + 1];
                allExps[0] = exp;
                System.arraycopy(exps, 0, allExps, 1, exps.length);
                policy.filterExp = Exp.build(Exp.and(allExps));
            }
        }

        if (mapKey != null) {
            db.getScanHitCounter().increment(mapKey);
        }

        final PageFetcher<E> pageFetcher = new ScanPageFetcher(graph, policy, setName, db.PAGINATION_PAGE_SIZE, mapKey, transform);
        return pageFetcher.startQuery();
    }

    ///////////////////////////
    // Secondary index methods.
    ///////////////////////////

    public <E> Iterator<E> querySIndex(final String setName,
                                       final String indexName,
                                       final Filter filter,
                                       final QueryPolicy policy,
                                       final FireflyGraph.TransformKeyRecord<E> transformKeyRecord) {
        final PageFetcher<E> pageFetcher = new SindexPageFetcher<>(
                graph, policy, setName, db.getNamespace(), filter, db.PAGINATION_PAGE_SIZE, transformKeyRecord, indexName);
        return pageFetcher.startQuery();
    }

    public <E> Iterator<E> queryVertexSIndex(final FireflyIndexMetadata.IndexInfo indexInfo,
                                             final P<?> predicate,
                                             final FireflyGraph.TransformKeyRecord<E> transform,
                                             final Long evaluationTimeout) {
        return queryVertexSIndex(indexInfo, predicate, transform, Collections.emptyList(), evaluationTimeout);
    }

    public <E> Iterator<E> queryVertexSIndex(final FireflyIndexMetadata.IndexInfo indexInfo,
                                             final P<?> predicate,
                                             final FireflyGraph.TransformKeyRecord<E> transform,
                                             final List<HasContainer> hasContainers,
                                             final Long evaluationTimeout) {
        // Create query policy with expressions.
        final QueryPolicy queryPolicy = new QueryPolicy();
        graph.getBaseGraph().configureIndexPolicy(queryPolicy);
        queryPolicy.filterExp = GraphQueryHelper.hasContainerListToExpression(graph.getBaseGraph(), hasContainers, FireflyVertex.class);

        // Override with evaluationTimeout if < default.
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
                GraphQueryHelper.predicateToFilter(graph.getBaseGraph(), predicate, indexInfo), queryPolicy, transform);
    }

    public <E> Iterator<E> querySIndex(String setName,
                                       String indexName,
                                       Filter filter,
                                       QueryPolicy policy) {
        return (Iterator<E>) querySIndex(setName, indexName, filter, policy, (it) -> it);
    }

    ////////////////////
    // Computer Methods.
    ////////////////////

    public <E> BlockingQueue<PageFetcher.Page> batchReadVertexPagesBlocking(final FireflyGraph graph,
                                                                            final Expression expression,
                                                                            final FireflyGraph.TransformKeyRecord<E> transformKeyRecord,
                                                                            final List<Object> idsToRead,
                                                                            final Long evaluationTimeout) {

        if (idsToRead.size() == 1 && idsToRead.get(0) instanceof P) {
            // Passed in as P.within([id1, id2, ...])
            final P p = (P) idsToRead.get(0);
            if (!p.getBiPredicate().toString().equals("within")) {
                throw new IllegalArgumentException("Batch read only supports within predicate");
            }
            if (!(p.getValue() instanceof List)) {
                throw new IllegalArgumentException("Batch read only supports a single list of keys");
            }
            idsToRead.clear();
            idsToRead.addAll((List) p.getValue());
        }

        final List<Key> keysToRead = idsToRead.stream().
                map(id -> graph.getIdFactory().createVertexId(id)).
                map(vertexId -> getKey(graph.getBaseGraph(), graph.getBaseGraph().VERTEX_AERO_SET, vertexId)).
                collect(Collectors.toList());

        final PageFetcher<E> pageFetcher = new BatchReadPageFetcher<>(
                graph, db.PAGINATION_PAGE_SIZE, expression, transformKeyRecord, keysToRead, evaluationTimeout);
        return pageFetcher.startQueryDirect();
    }

    // Unused right now. Could be used later for quicker scan counting.
    public BlockingQueue<PageFetcher.Page> scanVertexIdPages(final List<HasContainer> hasContainers,
                                                             final Long evaluationTimeout) {
        final P<?> predicate;
        final String binName;
        final String mapKey;
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

    // GRAPH-1380 - Figure out dynamic paging.
    public BlockingQueue<PageFetcher.Page> partitionVertices(final List<FireflyVertex> vertices) {
        final BlockingQueue<PageFetcher.Page> pages = new LinkedBlockingQueue<>();
        Lists.partition(vertices, ConfigurationHelper.getOrDefaultInt(ConfigurationHelper.Keys.PAGINATION_PAGE_SIZE, graph.configuration()))
                .forEach(list -> pages.add(new PageFetcher.VertexPage(CloseableIterator.of(list.iterator()))));
        pages.add(new PageFetcher.PoisonPill());
        return pages;
    }

    public BlockingQueue<PageFetcher.Page> partitionVertexIdPages(final List<HasContainer> hasContainers, final Long evaluationTimeout) {
        P<?> predicate = null;
        String binName = null;
        String mapKey = null;
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

        // to scan.
        return scanSetPagesBlocking(mapKey, db.VERTEX_AERO_SET, binName, predicate, graph::vertexIdFromRecord,
                hasContainers, FireflyVertex.class, true, true, evaluationTimeout);
    }

    public <E> BlockingQueue<PageFetcher.Page> scanSetPagesBlocking(final String mapKey, final String setName, final String binName, final P<?> predicate,
                                                                    final FireflyGraph.TransformKeyRecord<E> transform, final List<HasContainer> hasContainers,
                                                                    final Class<? extends FireflyElement> clazz, final boolean sendKey, final boolean includeBinData,
                                                                    final Long evaluationTimeout, final String... binNames) {
        final ScanPolicy policy = new ScanPolicy();
        policy.sendKey = sendKey;
        policy.includeBinData = includeBinData;
        policy.setTimeout(evaluationTimeout.intValue());

        // Build expression using predicate.
        if (predicate != null) {
            final Exp exp = GraphQueryHelper.predicateToExpression(db, binName, mapKey, predicate);
            if (!hasContainers.isEmpty()) {
                final Exp[] exps = GraphQueryHelper.hasContainerListToExpArray(db, hasContainers, clazz);
                final Exp[] allExps = new Exp[exps.length + 1];
                allExps[0] = exp;
                System.arraycopy(exps, 0, allExps, 1, exps.length);
                policy.filterExp = Exp.build(Exp.and(allExps));
            } else {
                policy.filterExp = Exp.build(exp);
            }
        }

        if (mapKey != null) {
            db.getScanHitCounter().increment(mapKey);
        }

        final PageFetcher pageFetcher = new ScanPageFetcher(graph, policy, setName, db.PAGINATION_PAGE_SIZE, mapKey, transform);
        return pageFetcher.startQueryDirect();
    }


    public <E> BlockingQueue<PageFetcher.Page> indexSetPagesBlocking(final String setName,
                                                                     final String indexName,
                                                                     final Filter filter,
                                                                     final QueryPolicy policy,
                                                                     final FireflyGraph.TransformKeyRecord<E> transformKeyRecord) {
        final int partitions = 4096; // Always 4096 in Aerospike. Like the speed of light, this is constant.
        final ExecutorService readLoopExecutorService = Executors.newFixedThreadPool(db.PAGINATION_WORKERS, r -> {
            final Thread t = new Thread(r);
            t.setName("Aerospike-Graph-Partition-Worker-" + t.getId());
            t.setDaemon(true);
            return t;
        });
        final LinkedBlockingQueue<PageFetcher.Page> pageQueue = new LinkedBlockingQueue<>();
        final Object lock = new Object();
        final List<AtomicBoolean> allCompleted = new ArrayList<>();
        final List<Range> ranges = Range.splitPartitions(partitions, db.PAGINATION_WORKERS);
        for (final Range range : ranges) {
            final PartitionFilter partitionFilter = PartitionFilter.range(range.start, range.count);
            final PageFetcher<E> pageFetcher = new PartitionedSindexPageFetcher<>(
                    graph, policy, setName, db.getNamespace(), filter, db.PAGINATION_PAGE_SIZE, transformKeyRecord, indexName,
                    partitionFilter,readLoopExecutorService ,
                    pageQueue, lock, allCompleted, db.PAGINATION_WORKERS);

            // Intentionally not using return value here.
            pageFetcher.startQueryDirect();
        }
        return pageQueue;
    }

    static class Range {
        private final int start;
        private final int count;

        public Range(final int start, final int count) {
            this.start = start;
            this.count = count;
        }

        public static List<Range> splitPartitions(int totalPartitions, int numberOfWorkers) {
            final List<Range> ranges = new ArrayList<>();
            final int partitionsPerWorker = totalPartitions / numberOfWorkers;
            int remainder = totalPartitions % numberOfWorkers;

            int count;
            for (int start = 0; start < totalPartitions; start+=count) {
                count = partitionsPerWorker;

                // Distribute the remainder
                if (remainder > 0) {
                    count++;
                    remainder--;
                }
                ranges.add(new Range(start, count));
            }

            return ranges;
        }

        @Override
        public String toString() {
            return "[" + start + " - " + (start + count - 1) + "]";
        }
    }
}
