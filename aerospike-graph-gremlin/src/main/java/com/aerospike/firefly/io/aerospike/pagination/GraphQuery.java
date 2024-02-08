package com.aerospike.firefly.io.aerospike.pagination;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.iterator.FireflyPhatEdgeIdIterator;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
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
        return scanAllElementIds(FireflyVertex.class, List.of());
    }

    public Iterator<FireflyId> getPagedScanEdgeIds() {
        return scanAllElementIds(FireflyEdge.class, List.of());
    }

    public Iterator<FireflyId> getPagedScanVertexIds(final List<HasContainer> hasContainers) {
        return scanAllElementIds(FireflyVertex.class, hasContainers);
    }

    private Iterator<FireflyId> scanAllElementIds(final Class<? extends FireflyElement> clazz, final List<HasContainer> hasContainers) {
        final P<?> predicate = hasContainers.isEmpty() ? null : hasContainers.remove(0).getPredicate();
        if (FireflyVertex.class.isAssignableFrom(clazz)) {
            return getPagedScan(
                    null,
                    db.VERTEX_AERO_SET,
                    null,
                    predicate,
                    graph::vertexIdFromRecord,
                    hasContainers,
                    null,
                    true,
                    false);
        } else if (FireflyEdge.class.isAssignableFrom(clazz)) {
            return new FireflyPhatEdgeIdIterator(getPagedScan(
                    null,
                    db.VERTEX_AERO_SET,
                    null,
                    predicate,
                    graph::keyRecordFromKeyRecord,
                    hasContainers,
                    null,
                    true,
                    true), db);
        } else {
            throw new IllegalArgumentException("Cannot scan all element ids for unknown class: " + clazz);
        }
    }

    public <E> Iterator<E> getPagedScan(final String mapKey,
                                        final String setName,
                                        final String binName,
                                        final P<?> predicate,
                                        final FireflyGraph.TransformKeyRecord<E> transform) {
        return getPagedScan(mapKey, setName, binName, predicate, transform, List.of(), FireflyVertex.class, true, true);
    }

    public <E> Iterator<E> getPagedScan(final String mapKey,
                                        final String setName,
                                        final String binName,
                                        final P<?> predicate,
                                        final FireflyGraph.TransformKeyRecord<E> transform,
                                        final List<HasContainer> hasContainers,
                                        final Class<? extends FireflyElement> clazz,
                                        final boolean sendKey,
                                        final boolean includeBinData,
                                        String... binNames) {
        final ScanPolicy policy = new ScanPolicy();
        policy.sendKey = sendKey;
        policy.includeBinData = includeBinData;
        // Build expression using predicate.
        if (predicate != null) {
            final Exp exp = predicateToExpression(binName, mapKey, predicate);
            if (!hasContainers.isEmpty()) {
                final Exp[] exps = hasContainerListToExpArray(hasContainers, clazz);
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

        // TODO: Need scan info here for uuid thing.


        LOG.debug("Issuing scan query of all records in {}:{}:{} with filter {}.", db.getNamespace(), setName, Arrays.toString(binNames), policy.filterExp);
        final PageFetcher pageFetcher = new ScanPageFetcher(graph,
                policy,
                setName,
                db.getNamespace(),
                db.PAGINATION_READ_THREAD_COUNT,
                db.PAGINATION_PAGE_QUEUE_SIZE,
                db.PAGINATION_PAGE_SIZE,
                transform);
        return pageFetcher.startQuery();
    }


    /**
     * Create an Aerospike index Filter using the predicate and index info.
     *
     * @param predicate Predicate to use.
     * @param indexInfo Index info to use.
     * @return
     */
    public Filter predicateToFilter(final P<?> predicate, final FireflyIndexMetadata.IndexInfo indexInfo) {
        final String name;
        if (db.LABEL_BIN.equals(indexInfo.key)) {
            name = db.LABEL_BIN;
        } else if (indexInfo.setName.equals(db.VERTEX_AERO_SET)) {
            name = db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN;
        } else if (indexInfo.setName.equals(db.EDGE_AERO_SET)) {
            name = db.PROPERTIES_BIN;
        } else {
            throw new IllegalArgumentException(
                    "Cannot create filter for index with unknown set name: " + indexInfo.setName + " and key " + indexInfo.key);
        }

        final IndexCollectionType type = db.LABEL_BIN.equals(indexInfo.key) ?
                IndexCollectionType.DEFAULT : IndexCollectionType.MAPVALUES;
        final Object value = predicate.getValue();
        if (Number.class.isAssignableFrom(value.getClass())) {
            final Long casted;
            if (Integer.class.isAssignableFrom(value.getClass())) {
                casted = Long.valueOf((Integer) value);
            } else if (Long.class.isAssignableFrom(value.getClass())) {
                casted = (Long) value;
            } else {
                throw new RuntimeException(String.format("%s not a supported numeric type", predicate.getValue().getClass()));
            }

            if (predicate.getBiPredicate().equals(Compare.eq)) {
                return Filter.equal(name, casted, CTX.mapKey(Value.get(indexInfo.key)));
            } else if (predicate.getBiPredicate().equals(Compare.lt)) {
                return Filter.range(name, Long.MIN_VALUE, casted - 1, CTX.mapKey(Value.get(indexInfo.key)));
            } else if (predicate.getBiPredicate().equals(Compare.lte)) {
                return Filter.range(name, Long.MIN_VALUE, casted, CTX.mapKey(Value.get(indexInfo.key)));
            } else if (predicate.getBiPredicate().equals(Compare.gt)) {
                return Filter.range(name, casted - 1, Long.MAX_VALUE, CTX.mapKey(Value.get(indexInfo.key)));
            } else if (predicate.getBiPredicate().equals(Compare.gte)) {
                return Filter.range(name, casted, Long.MAX_VALUE, CTX.mapKey(Value.get(indexInfo.key)));
            } else {
                throw new RuntimeException(String.format("%s not a supported predicate", predicate));
            }
        } else {
            if (db.LABEL_BIN.equals(indexInfo.key)) {
                return Filter.contains(name, type, (String) value);
            } else {
                return Filter.equal(name, (String) value, CTX.mapKey(Value.get(indexInfo.key)));
            }
        }
    }

    /**
     * Create an Aerospike Expression from the predicate, map key, and bin name.
     *
     * @param binName   Bin name to use.
     * @param mapKey    Map key to use.
     * @param predicate Predicate to use.
     * @return Expression.
     */
    public Exp predicateToExpression(final String binName,
                                     final String mapKey,
                                     final P<?> predicate) {
        // If the bin is the label bin, we can make a very simple predicate.
        if (db.LABEL_BIN.equals(binName)) {
            return Exp.eq(Exp.stringBin(db.LABEL_BIN), Exp.val((String) predicate.getValue()));
        }

        // Need to build a more complex expression for nested map values.
        final Object value = predicate.getValue();
        if (Number.class.isAssignableFrom(value.getClass())) {
            final Long casted;
            if (Integer.class.isAssignableFrom(value.getClass())) {
                casted = Long.valueOf((Integer) value);
            } else if (Long.class.isAssignableFrom(value.getClass())) {
                casted = (Long) value;
            } else {
                throw new RuntimeException(String.format("%s not a supported numeric type", predicate.getValue().getClass()));
            }

            if (predicate.getBiPredicate().equals(Compare.eq)) {
                return Exp.eq(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(mapKey), Exp.mapBin(binName)), Exp.val(casted));
            } else if (predicate.getBiPredicate().equals(Compare.lt)) {
                return Exp.lt(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(mapKey), Exp.mapBin(binName)), Exp.val(casted));
            } else if (predicate.getBiPredicate().equals(Compare.lte)) {
                return Exp.le(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(mapKey), Exp.mapBin(binName)), Exp.val(casted));
            } else if (predicate.getBiPredicate().equals(Compare.gt)) {
                return Exp.gt(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(mapKey), Exp.mapBin(binName)), Exp.val(casted));
            } else if (predicate.getBiPredicate().equals(Compare.gte)) {
                return Exp.ge(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.INT, Exp.val(mapKey), Exp.mapBin(binName)), Exp.val(casted));
            } else {
                throw new RuntimeException(String.format("%s not a supported predicate", predicate));
            }
        } else {
            return Exp.eq(MapExp.getByKey(MapReturnType.VALUE, Exp.Type.STRING, Exp.val(mapKey), Exp.mapBin(binName)), Exp.val((String) value));
        }
    }

    public Expression hasContainerListToExpression(final List<HasContainer> hasContainers, final Class<? extends FireflyElement> clazz) {
        // If the key is ~label, then bin name is label, else it depends on whether this is vertex or edge.
        if (hasContainers.size() == 0) {
            return null;
        }
        final Exp[] exps = hasContainers.stream().map(h ->
                predicateToExpression(h.getKey().equals("~label") ?
                                db.LABEL_BIN : FireflyVertex.class.isAssignableFrom(clazz) ?
                                db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN : db.PROPERTIES_BIN,
                        h.getKey(),
                        h.getPredicate())).toArray(Exp[]::new);
        return exps.length == 1 ? Exp.build(exps[0]) : Exp.build(Exp.and(exps));
    }

    private Exp[] hasContainerListToExpArray(final List<HasContainer> hasContainers, final Class<? extends FireflyElement> clazz) {
        // If the key is ~label, then bin name is label, else it depends on whether this is vertex or edge.
        return hasContainers.stream().map(h ->
                predicateToExpression(h.getKey().equals("~label") ?
                                db.LABEL_BIN : FireflyVertex.class.isAssignableFrom(clazz) ?
                                db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN : db.PROPERTIES_BIN,
                        h.getKey(),
                        h.getPredicate())).toArray(Exp[]::new);
    }
}
