package com.aerospike.firefly.io.aerospike.query.paged;

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
import org.apache.commons.lang3.function.TriFunction;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;

public class VertexQueryHelper {

    protected static final Map<PBiPredicate, BiFunction<Exp, Exp, Exp>> COMPARE_TO_EXP = Map.of(
            Compare.eq, Exp::eq,
            Compare.neq, Exp::ne,
            Compare.lt, Exp::lt,
            Compare.lte, Exp::le,
            Compare.gt, Exp::gt,
            Compare.gte, Exp::ge//,
            //Contains.within, (exp1, exp2) -> Exp.or(Exp.in(exp1, exp2), Exp.in(exp2, exp1))
    );

    protected static Long castLong(Object value) {
        if (value instanceof Number) {
            return ((Number) value).longValue();
        } else {
            throw new RuntimeException(String.format("%s not a supported numeric type", value.getClass()));
        }
    }

    protected static final Map<PBiPredicate, TriFunction<String, Object, CTX[], Filter>> COMPARE_TO_FILTER = Map.of(
            Compare.eq, (name, value, ctx) -> (value instanceof String) ?
                    Filter.contains(name, IndexCollectionType.MAPKEYS, (String) value, ctx) :
                    Filter.contains(name, IndexCollectionType.MAPKEYS, castLong(value), ctx),
            Compare.lt, (name, value, ctx) -> Filter.range(name, IndexCollectionType.MAPKEYS, Long.MIN_VALUE, castLong(value) - 1, ctx),
            Compare.lte, (name, value, ctx) -> Filter.range(name, IndexCollectionType.MAPKEYS, Long.MIN_VALUE, castLong(value), ctx),
            Compare.gt, (name, value, ctx) -> Filter.range(name, IndexCollectionType.MAPKEYS, castLong(value) - 1, Long.MAX_VALUE, ctx),
            Compare.gte, (name, value, ctx) -> Filter.range(name, IndexCollectionType.MAPKEYS, castLong(value), Long.MAX_VALUE, ctx)
    );

    protected static Exp.Type getExpType(final Object value) {
        if (value == null) {
            return Exp.Type.NIL;
        }
        if (Number.class.isAssignableFrom(value.getClass())) {
            return Exp.Type.INT;
        }
        if (String.class.isAssignableFrom(value.getClass())) {
            return Exp.Type.STRING;
        }
        throw new RuntimeException(String.format("%s not a supported type for value in predicate", value.getClass()));
    }

    protected static Exp getValue(final Object value) {
        if (value == null) {
            return Exp.nil();
        }
        if (Number.class.isAssignableFrom(value.getClass())) {
            return Exp.val(((Number) value).longValue());
        }
        if (String.class.isAssignableFrom(value.getClass())) {
            return Exp.val((String) value);
        }
        throw new RuntimeException(String.format("%s not a supported type for value in predicate", value.getClass()));
    }

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
            name = db.VERTEX_PROPERTY_DATA_BIN;
        } else {
            throw new IllegalArgumentException(
                    "Cannot create filter for index with unknown set name: " + indexInfo.setName + " and key " + indexInfo.key);
        }

        final IndexCollectionType type = db.LABEL_BIN.equals(indexInfo.key) ?
                IndexCollectionType.DEFAULT : IndexCollectionType.MAPKEYS;
        final Object value = predicate.getValue();
        if (db.LABEL_BIN.equals(indexInfo.key)) {
            return Filter.contains(name, type, db.schemaManager.getVertexLabelRead(((String) value)));
        }
        final Long schemaKey = db.schemaManager.getVertexPropertyRead(indexInfo.key);
        return COMPARE_TO_FILTER.get(predicate.getBiPredicate())
                .apply(name, value, new CTX[]{CTX.mapKey(Value.get(schemaKey))});
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
            return Exp.eq(Exp.intBin(db.LABEL_BIN), Exp.val(db.schemaManager.getVertexLabelRead((String) predicate.getValue())));
        }
        return MapExp.getByKey(MapReturnType.EXISTS,
                        Exp.Type.BOOL,
                        getValue(predicate.getValue()),
                        Exp.mapBin(binName),
                        CTX.mapKey(Value.get(db.schemaManager.getVertexPropertyRead(mapKey))));
        //return COMPARE_TO_EXP.get(predicate.getBiPredicate())
        //        .apply(MapExp.getByKey(MapReturnType.VALUE,
        //                        getExpType(predicate.getValue()),
        //                        Exp.val(db.schemaManager.getVertexPropertyRead(mapKey)),
        //                        Exp.mapBin(binName)),
        //                getValue(predicate.getValue()));
    }

    public static Expression hasContainerListToExpression(final AerospikeConnection db, final List<HasContainer> hasContainers) {
        if (hasContainers.isEmpty() && !db.TTL_ENABLED_FLAG) {
            return null;
        }
        final List<Exp> exps = new ArrayList<>();
        if (db.TTL_ENABLED_FLAG) {
            final Exp ttlExp = getVertexTtlExp(db);
            exps.add(ttlExp);
        }

        for (final HasContainer h : hasContainers) {
            final Exp expFromPredicate = predicateToExpression(db,
                    h.getKey().equals("~label") ? db.LABEL_BIN : db.VERTEX_PROPERTY_DATA_BIN,
                    h.getKey(), h.getPredicate());
            exps.add(expFromPredicate);
        }
        return exps.size() == 1 ? Exp.build(exps.get(0)) : Exp.build(Exp.and(exps.toArray(new Exp[0])));
    }

    public static Exp[] hasContainerListToExpArray(final AerospikeConnection db, final List<HasContainer> hasContainers, final Class<? extends FireflyElement> clazz) {
        // If the key is ~label, then bin name is label, else it depends on whether this is vertex or edge.
        if (!FireflyVertex.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException("Cannot push predicates down to: " + clazz);
        }
        final List<Exp> exps = new ArrayList<>();
        if (db.TTL_ENABLED_FLAG) {
            final Exp ttlExp = getVertexTtlExp(db);
            exps.add(ttlExp);
        }
        for (final HasContainer h : hasContainers) {
            final Exp expFromPredicate = predicateToExpression(db,
                    h.getKey().equals("~label") ? db.LABEL_BIN : db.VERTEX_PROPERTY_DATA_BIN,
                    h.getKey(), h.getPredicate());
            exps.add(expFromPredicate);
        }
        return exps.toArray(new Exp[0]);
    }

    private static Exp getVertexTtlExp(final AerospikeConnection db) {
        return Exp.or(
                Exp.gt(Exp.intBin(db.TTL_BIN), Exp.val(System.currentTimeMillis())),
                Exp.not(
                        Exp.binExists(db.TTL_BIN)
                )
        );
    }
}
