package com.aerospike.firefly.io.aerospike.pagination;

import com.aerospike.client.exp.Exp;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIterator;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class GraphQuery {
    private static final Logger LOG = LoggerFactory.getLogger(GraphQuery.class);
    final FireflyGraph graph;
    final AerospikeConnection db;

    public GraphQuery(final FireflyGraph graph) {
        this.graph = graph;
        this.db = graph.getBaseGraph();
    }

    public Iterator<FireflyId> getPagedScanVertexIds() {
        return getPagedScanOfElementIds(FireflyVertex.class, List.of());
    }

    public Iterator<FireflyId> getPagedScanEdgeIds() {
        return getPagedScanOfElementIds(FireflyEdge.class, List.of());
    }

    public Iterator<FireflyId> getPagedScanVertexIds(final List<HasContainer> hasContainers) {
        return getPagedScanOfElementIds(FireflyVertex.class, hasContainers);
    }

    private Iterator<FireflyId> getPagedScanOfElementIds(final Class<? extends FireflyElement> clazz, final List<HasContainer> hasContainers) {
        final P<?> predicate;
        final String binName;
        final String mapKey;
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
            return getPagedScan(mapKey, db.VERTEX_AERO_SET, binName, predicate, graph::vertexIdFromRecord,
                    hasContainers, clazz, true, true);
        } else if (FireflyEdge.class.isAssignableFrom(clazz)) {
            return new FireflyPhatEdgeIdIterator(getPagedScan(
                    mapKey, db.EDGE_AERO_SET, binName, predicate, graph::keyRecordFromKeyRecord,
                    hasContainers, clazz, true, true), db);
        } else {
            throw new IllegalArgumentException("Cannot scan all element ids for unknown class: " + clazz);
        }
    }

    public <E> Iterator<E> getPagedScan(final String mapKey, final String setName, final String binName, final P<?> predicate,
                                        final FireflyGraph.TransformKeyRecord<E> transform) {
        return getPagedScan(mapKey, setName, binName, predicate, transform, List.of(), FireflyVertex.class, true, true);
    }

    public <E> Iterator<E> getPagedScan(final String mapKey, final String setName, final String binName, final P<?> predicate,
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

    public <E> Iterator<E> getPagedSindex(final FireflyIndexMetadata.IndexInfo indexInfo, final P<?> predicate, final FireflyGraph.TransformKeyRecord<E> transform) {
        return getPagedSindex(indexInfo, predicate, transform, Collections.emptyList(), null);
    }


    public <E> Iterator<E> getPagedSindex(final FireflyIndexMetadata.IndexInfo indexInfo, final P<?> predicate,
            final FireflyGraph.TransformKeyRecord<E> transform, final List<HasContainer> hasContainers, final Class<? extends FireflyElement> clazz) {
        // Create query policy with expressions.
        final QueryPolicy queryPolicy = new QueryPolicy();
        queryPolicy.filterExp = GraphQueryHelper.hasContainerListToExpression(db, hasContainers, clazz);

        // Query index.
        return getPagedSindex(indexInfo.setName, indexInfo.indexName, GraphQueryHelper.predicateToFilter(db, predicate, indexInfo), queryPolicy, transform);
    }

    public <E> Iterator<E> getPagedSindex(final String setName, final String indexName, final Filter filter, final QueryPolicy policy) {
        return (Iterator<E>) getPagedSindex(setName, indexName, filter, policy, graph::keyRecordFromKeyRecord);
    }

    public <E> Iterator<E> getPagedSindex(final String setName, final String indexName, final Filter filter, final QueryPolicy policy, final FireflyGraph.TransformKeyRecord<E> transformKeyRecord) {
        final PageFetcher<E> pageFetcher = new SindexPageFetcher<>(graph, policy, setName, db.getNamespace(), filter, db.PAGINATION_PAGE_QUEUE_SIZE, db.PAGINATION_PAGE_SIZE, transformKeyRecord);
        return pageFetcher.startQuery();
    }
}
