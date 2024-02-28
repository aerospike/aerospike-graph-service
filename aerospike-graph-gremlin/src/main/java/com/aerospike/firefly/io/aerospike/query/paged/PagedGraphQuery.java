package com.aerospike.firefly.io.aerospike.query.paged;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.io.aerospike.query.GraphQuery;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

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
                                   final String... binNames) {
        final ScanPolicy policy = new ScanPolicy();
        policy.sendKey = sendKey;
        policy.includeBinData = includeBinData;
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
        final PageFetcher pageFetcher = new ScanPageFetcher(graph,
                policy,
                setName,
                db.getNamespace(),
                db.PAGINATION_PAGE_QUEUE_SIZE,
                db.PAGINATION_PAGE_SIZE,
                mapKey,
                transform);
        return pageFetcher.startQuery();
    }


    @Override
    public <E> Iterator<E> querySIndex(final String setName,
                                       final String indexName,
                                       final Filter filter,
                                       final QueryPolicy policy,
                                       final FireflyGraph.TransformKeyRecord<E> transformKeyRecord) {
        final PageFetcher<E> pageFetcher = new SindexPageFetcher<>(graph, policy, setName, db.getNamespace(), filter, db.PAGINATION_PAGE_QUEUE_SIZE, db.PAGINATION_PAGE_SIZE, transformKeyRecord);
        return pageFetcher.startQuery();
    }

}
