package com.aerospike.firefly.io.aerospike.pagination;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyVertex;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.List;

public class GraphQueryHelper {

    /**
     * Create an Aerospike index Filter using the predicate and index info.
     *
     * @param predicate Predicate to use.
     * @param indexInfo Index info to use.
     * @return
     */
    public static Filter predicateToFilter(final AerospikeConnection db, final P<?> predicate, final FireflyIndexMetadata.IndexInfo indexInfo) {
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
    public static Exp predicateToExpression(final AerospikeConnection db,
                                            final String binName,
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

    public static Expression hasContainerListToExpression(final AerospikeConnection db, final List<HasContainer> hasContainers, final Class<? extends FireflyElement> clazz) {
        // If the key is ~label, then bin name is label, else it depends on whether this is vertex or edge.
        if (hasContainers.size() == 0) {
            return null;
        }
        final Exp[] exps = hasContainers.stream().map(h ->
                predicateToExpression(db, h.getKey().equals("~label") ?
                                db.LABEL_BIN : FireflyVertex.class.isAssignableFrom(clazz) ?
                                db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN : db.PROPERTIES_BIN,
                        h.getKey(),
                        h.getPredicate())).toArray(Exp[]::new);
        return exps.length == 1 ? Exp.build(exps[0]) : Exp.build(Exp.and(exps));
    }

    static Exp[] hasContainerListToExpArray(final AerospikeConnection db, final List<HasContainer> hasContainers, final Class<? extends FireflyElement> clazz) {
        // If the key is ~label, then bin name is label, else it depends on whether this is vertex or edge.
        return hasContainers.stream().map(h ->
                predicateToExpression(db, h.getKey().equals("~label") ?
                                db.LABEL_BIN : FireflyVertex.class.isAssignableFrom(clazz) ?
                                db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN : db.PROPERTIES_BIN,
                        h.getKey(),
                        h.getPredicate())).toArray(Exp[]::new);
    }
}
