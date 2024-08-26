package com.aerospike.firefly.io.aerospike.query.legacy;

import com.aerospike.client.async.Monitor;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.policy.BatchPolicy;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.KeyRecord;
import com.aerospike.client.query.Statement;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.ScanHitCounter;
import com.aerospike.firefly.io.aerospike.query.GraphQuery;
import com.aerospike.firefly.io.aerospike.query.paged.GraphQueryHelper;
import com.aerospike.firefly.io.aerospike.query.paged.PageFetcher;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.iterator.FireflyCloseableIterator;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;

import static com.aerospike.firefly.io.aerospike.AerospikeConnection.DefaultAerospikeClientProvider.client;

public class LegacyGraphQuery implements GraphQuery {
    private final FireflyGraph fireflyGraph;
    private final AerospikeConnection db;


    public LegacyGraphQuery(FireflyGraph fireflyGraph) {
        this.fireflyGraph = fireflyGraph;
        this.db = fireflyGraph.getBaseGraph();

    }


    @Override
    public <E> Iterator<E> querySIndex(final String setName,
                                       final String indexName,
                                       final Filter filter,
                                       final QueryPolicy policy,
                                       final FireflyGraph.TransformKeyRecord<E> transformKeyRecord) {

        final Statement statement = new Statement();
        statement.setNamespace(db.namespace);
        statement.setSetName(setName);
        statement.setIndexName(indexName);
        statement.setFilter(filter);
        db.configureReadPolicy(policy);

        return IteratorUtils.
                stream(fireflyGraph.getBaseGraph().client.query(policy, statement))
                .map( item -> transformKeyRecord.transform(item))
                .iterator();
    }

    @Override
    public FireflyGraph getGraph() {
        return fireflyGraph;
    }

    private Iterator<KeyRecord> scanAllRecordsInSet(final String setName,
                                                   final String mapKey,
                                                   final ScanPolicy policy,
                                                   final boolean sendKey,
                                                   final String... binNames) {
        final Monitor scanMonitor = new Monitor();
        policy.sendKey = sendKey;
        db.configureScanPolicy(policy);
        final UUID scanId = UUID.randomUUID();
        final ScanHitCounter shc = db.getScanHitCounter();
        if(mapKey != null) shc.associateUUID(scanId, mapKey);
        final ConcurrentScanRecordSequenceListener listener =
                ConcurrentScanRecordSequenceListener.create(db, scanMonitor, scanId);
        listener.setStartTime();
        client.scanAll(db.getEventLoops().next(), listener, policy, db.getNamespace(), setName, binNames);

        return new FireflyCloseableIterator<>(listener.iterator());
    }


    @Override
    public <E> BlockingQueue<PageFetcher.Page> scanSetPagesBlocking(final String mapKey, final String setName, final String binName, final P<?> predicate,
                                                                    final FireflyGraph.TransformKeyRecord<E> transform, final List<HasContainer> hasContainers,
                                                                    final Class<? extends FireflyElement> clazz, final boolean sendKey, final boolean includeBinData,
                                                                    final Optional<Long> evaluationTimeout,
                                                                    final String... binNames) {
        throw new RuntimeException("The graph computer does not support legacy reading.");
    }


    @Override
    public <E> BlockingQueue<PageFetcher.Page> batchReadSetPagesBlocking(final FireflyGraph graph, BatchPolicy policy,
                                                                         final Class<? extends FireflyElement> type,
                                                                         final Expression expression,
                                                                         final FireflyGraph.TransformKeyRecord<E> transformKeyRecord,
                                                                         final List<Object> idsToRead) {
        throw new RuntimeException("The graph computer does not support legacy reading.");
    }

    @Override
    public <E> Iterator<E> scanSet(final String mapKey,
                                   final String setName,
                                   final String binName,
                                   final P<?> predicate,
                                   final FireflyGraph.TransformKeyRecord<E> transform,
                                   final List<HasContainer> hasContainers,
                                   final Class<? extends FireflyElement> clazz,
                                   final boolean sendKey,
                                   final boolean includeBinData,
                                   Optional<Long> evaluationTimeout,
                                   final String... binNames) {
        final ScanPolicy policy = new ScanPolicy();
        policy.sendKey = sendKey;
        policy.includeBinData = includeBinData;
        policy.setTimeout(evaluationTimeout.orElse(fireflyGraph.settings().evaluationTimeout).intValue());
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
        return (Iterator<E>) IteratorUtils.map(scanAllRecordsInSet(setName,mapKey,policy,sendKey,binNames), it -> transform.transform(it));
    }

    @Override
    public <E> BlockingQueue<PageFetcher.Page> indexSetPagesBlocking(final String setName,
                                                                     final String indexName,
                                                                     final Filter filter,
                                                                     final QueryPolicy policy,
                                                                     final FireflyGraph.TransformKeyRecord<E> transformKeyRecord) {
        throw new RuntimeException("The graph computer does not support legacy reading.");
    }


    @Override
    public <E> Iterator<E> queryVertexSIndex(final FireflyIndexMetadata.IndexInfo indexInfo,
                                             final P<?> predicate,
                                             final FireflyGraph.TransformKeyRecord<E> transform,
                                             final List<HasContainer> hasContainers,
                                             final Optional<Long> evaluationTimeout) {
        // Create query policy with expressions.
        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.filterExp = GraphQueryHelper.hasContainerListToExpression(db, hasContainers, FireflyVertex.class);
        queryPolicy.setTimeout(evaluationTimeout.orElse(fireflyGraph.settings().evaluationTimeout).intValue());

        return querySIndex(indexInfo.setName, indexInfo.indexName, GraphQueryHelper.predicateToFilter(db, predicate, indexInfo),
                queryPolicy, transform);
   }
}
