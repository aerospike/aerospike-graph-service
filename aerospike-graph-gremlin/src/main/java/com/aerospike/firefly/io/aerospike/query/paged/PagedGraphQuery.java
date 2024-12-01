package com.aerospike.firefly.io.aerospike.query.paged;

import com.aerospike.client.Key;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.PartitionFilter;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.query.GraphQuery;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static com.aerospike.firefly.io.FireflyRecord.getKey;
import static com.aerospike.firefly.io.FireflyRecord.read;

public class PagedGraphQuery implements GraphQuery {
    private static final Logger LOG = LoggerFactory.getLogger(PagedGraphQuery.class);
    final FireflyGraph graph;
    final AerospikeConnection db;

    public PagedGraphQuery(final FireflyGraph graph) {
        this.graph = graph;
        this.db = graph.getBaseGraph();
    }

    @Override
    public FireflyGraph getGraph() {
        return graph;
    }

    @Override
    public <E> Iterator<E> scanSet(final String mapKey, final String setName, final String binName, final P<?> predicate,
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

        LOG.debug("Issuing scan query of all records in {}:{}:{} with filter {}.", db.getNamespace(), setName, Arrays.toString(binNames), policy.filterExp);
        final ExecutorService readLoopExecutorService = Executors.newSingleThreadExecutor();
        final BlockingQueue<PageFetcher.Page> pageQueue = new LinkedBlockingQueue<>();
        final Object lock = new Object();
        final List<AtomicBoolean> allCompleted = new ArrayList<>();
        final PageFetcher pageFetcher = new ScanPageFetcher(graph,
                lock,
                allCompleted,
                policy,
                setName,
                db.getNamespace(),
                db.PAGINATION_PAGE_SIZE,
                mapKey,
                transform,
                readLoopExecutorService,
                pageQueue);
        return pageFetcher.startQuery();
    }

    @Override
    public <E> BlockingQueue<PageFetcher.Page> scanSetPagesBlocking(final String mapKey, final String setName, final String binName, final P<?> predicate,
                                                            final FireflyGraph.TransformKeyRecord<E> transform, final List<HasContainer> hasContainers,
                                                            final Class<? extends FireflyElement> clazz, final boolean sendKey, final boolean includeBinData,
                                                            final Long evaluationTimeout, final String... binNames) {
        System.out.println("!!!!!!!!!!!!!! Running scan");
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

        LOG.debug("Issuing scan query of all records in {}:{}:{} with filter {}.", db.getNamespace(), setName, Arrays.toString(binNames), policy.filterExp);
        final BlockingQueue<PageFetcher.Page> pageQueue = new LinkedBlockingQueue<>();
        final ExecutorService readLoopExecutorService = Executors.newSingleThreadExecutor();
        final Object lock = new Object();
        final List<AtomicBoolean> allCompleted = new ArrayList<>();
        final PageFetcher pageFetcher = new ScanPageFetcher(
                graph,
                lock,
                allCompleted,
                policy,
                setName,
                db.getNamespace(),
                db.PAGINATION_PAGE_SIZE,
                mapKey,
                transform,
                readLoopExecutorService,
                pageQueue);
        pageFetcher.startQueryPagesDirect();
        return pageQueue;
    }

    @Override
    public <E> BlockingQueue<PageFetcher.Page> indexSetPagesBlocking(final String setName,
                                                                    final String indexName,
                                                                    final Filter filter,
                                                                    final QueryPolicy policy,
                                                                    final FireflyGraph.TransformKeyRecord<E> transformKeyRecord) {
        System.out.println("!!!!!!!!!!!!!! Running sindex");
        final int partitions = 4096;
        final int blockSize = partitions / db.PAGINATION_WORKERS;
        final int remainder = partitions % db.PAGINATION_WORKERS;
        final ExecutorService readLoopExecutorService = Executors.newFixedThreadPool(db.PAGINATION_WORKERS, r -> {
            final Thread t = new Thread(r);
            t.setName("Aerospike-Graph-Partition-Worker-" + t.getId());
            t.setDaemon(true);
            return t;
        });
        final LinkedBlockingQueue<PageFetcher.Page> pageQueue = new LinkedBlockingQueue<>();
        final Object lock = new Object();
        final List<AtomicBoolean> allCompleted = new ArrayList<>();
        for (int i = 0; i < db.PAGINATION_WORKERS; i++) {
            final int start = i * blockSize;
            final int end = start + blockSize + (i == db.PAGINATION_WORKERS - 1 ? remainder : 0);
            final PartitionFilter partitionFilter = PartitionFilter.range(start, end);
            final PageFetcher<E> pageFetcher = new SindexPageFetcher<>(
                    graph,
                    lock,
                    allCompleted,
                    policy,
                    setName,
                    db.getNamespace(),
                    filter,
                    db.PAGINATION_PAGE_SIZE,
                    transformKeyRecord,
                    indexName,
                    partitionFilter,
                    readLoopExecutorService,
                    pageQueue);
            pageFetcher.startQueryPagesDirect();
        }
        return pageQueue;
    }


    @Override
    public <E> Iterator<E> querySIndex(final String setName,
                                       final String indexName,
                                       final Filter filter,
                                       final QueryPolicy policy,
                                       final FireflyGraph.TransformKeyRecord<E> transformKeyRecord) {
        final LinkedBlockingQueue<PageFetcher.Page> pageQueue = new LinkedBlockingQueue<>();
        final Object lock = new Object();
        final List<AtomicBoolean> allCompleted = new ArrayList<>();
        final PageFetcher<E> pageFetcher = new SindexPageFetcher<>(
                graph,
                lock,
                allCompleted,
                policy,
                setName,
                db.getNamespace(),
                filter,
                db.PAGINATION_PAGE_SIZE,
                transformKeyRecord,
                indexName,
                PartitionFilter.all(),
                Executors.newSingleThreadExecutor(),
                pageQueue);
        return pageFetcher.startQuery();
    }

    @Override
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

        final ExecutorService readLoopExecutorService = Executors.newSingleThreadExecutor();
        final LinkedBlockingQueue<PageFetcher.Page> pageQueue = new LinkedBlockingQueue<>();
        final Object lock = new Object();
        final List<AtomicBoolean> allCompleted = new ArrayList<>();
        final PageFetcher<E> pageFetcher = new BatchReadPageFetcher<>(
                graph,
                lock,
                allCompleted,
                db.PAGINATION_PAGE_SIZE,
                expression,
                transformKeyRecord,
                keysToRead,
                evaluationTimeout,
                readLoopExecutorService, pageQueue);
        pageFetcher.startQueryPagesDirect();
        return pageQueue;
    }
}
