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
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyElement;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_LABEL_KEY;

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
            throw new IllegalArgumentException("Cannot create filter for index for Edges.");
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
        if (!FireflyVertex.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException("Cannot push predicates down to: " + clazz);
        }
        if (hasContainers.size() == 0) {
            return null;
        }
        final Exp[] exps = hasContainers.stream().map(h ->
                predicateToExpression(db, h.getKey().equals("~label") ? db.LABEL_BIN : db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN,
                        h.getKey(), h.getPredicate())).toArray(Exp[]::new);
        return exps.length == 1 ? Exp.build(exps[0]) : Exp.build(Exp.and(exps));
    }

    public static Exp[] hasContainerListToExpArray(final AerospikeConnection db, final List<HasContainer> hasContainers, final Class<? extends FireflyElement> clazz) {
        // If the key is ~label, then bin name is label, else it depends on whether this is vertex or edge.
        if (!FireflyVertex.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException("Cannot push predicates down to: " + clazz);
        }
        return hasContainers.stream().map(h ->
                predicateToExpression(db, h.getKey().equals("~label") ?
                                db.LABEL_BIN : db.VERTEX_PROPERTY_NAME_TO_VALUE_BIN,
                        h.getKey(), h.getPredicate())).toArray(Exp[]::new);
    }

    public static Expression phatEdgeHasContainerListToExpression(final AerospikeConnection db,
                                                                  final List<HasContainer> hasContainers,
                                                                  final Set<String> labels,
                                                                  final String vertexIdKeyHashString) {
        final Exp labelExp = getPhatEdgeLabelExp(db, labels, vertexIdKeyHashString);
        final Exp propertiesExp = getPhatEdgePropertyExp(db, hasContainers, vertexIdKeyHashString);
        if (labelExp != null && propertiesExp != null) {
            return Exp.build(Exp.and(labelExp, propertiesExp));
        } else if (labelExp != null) {
            return Exp.build(labelExp);
        } else if (propertiesExp != null ){
            return Exp.build(propertiesExp);
        } else {
            return null;
        }
    }

    private static Exp getPhatEdgeLabelExp(final AerospikeConnection db,
                                           final Set<String> labels,
                                           final String vertexIdKeyHashString) {
        if (labels.isEmpty()) {
            return null;
        } else {
            final List<Exp> allLabelExp = new ArrayList<>();
            for (final String label : labels) {
                final Exp labelExp = MapExp.getByValue(MapReturnType.EXISTS, Exp.val(label),
                        Exp.mapBin(db.SUPERNODE_EDGE_PROPERTIES_BIN), CTX.mapKey(Value.get(vertexIdKeyHashString)),
                        CTX.mapKey(Value.get(EDGE_SUPERNODE_LABEL_KEY)));
                allLabelExp.add(labelExp);
            }
            if (allLabelExp.size() == 1) {
                return allLabelExp.get(0);
            } else {
                return Exp.or(allLabelExp.toArray(new Exp[0]));
            }
        }
    }

    private static Exp getPhatEdgePropertyExp(final AerospikeConnection db,
                                              final List<HasContainer> hasContainers,
                                              final String vertexIdKeyHashString) {
        if (hasContainers.isEmpty()) {
            return null;
        }
        final PhatEdgeHasContainers phatEdgeHasContainers = new PhatEdgeHasContainers(hasContainers);
        final Exp[] exps = phatEdgeHasContainers.filteredHasContainers.stream().map(hasContainer ->
                        phatEdgePredicateToExp(db, vertexIdKeyHashString, hasContainer.getKey(), hasContainer.getPredicate()))
                .toArray(Exp[]::new);
        final Exp[] compoundExps = phatEdgeHasContainers.compoundHasContainers.entrySet().stream().map(compoundContainer ->
                        phatEdgeCompoundPredicateToExp(db, vertexIdKeyHashString, compoundContainer))
                .toArray(Exp[]::new);
        final Exp[] allExps = ArrayUtils.addAll(exps, compoundExps);
        if (allExps.length == 1) {
            return allExps[0];
        } else {
            return Exp.and(allExps);
        }
    }

    private static Exp phatEdgePredicateToExp(final AerospikeConnection db, final String vertexIdKeyHashString,
                                              final String propertyKey, final P<?> predicate) {
        // Build expression for nested Phat Edge properties.
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
                return MapExp.getByValue(MapReturnType.EXISTS, Exp.val(casted),
                        Exp.mapBin(db.SUPERNODE_EDGE_PROPERTIES_BIN), CTX.mapKey(Value.get(vertexIdKeyHashString)),
                        CTX.mapKey(Value.get(propertyKey)));
            } else if (predicate.getBiPredicate().equals(Compare.lt)) {
                // getByValueRange valueBegin is inclusive; valueEnd is exclusive.
                return MapExp.getByValueRange(MapReturnType.EXISTS, Exp.val(Long.MIN_VALUE), Exp.val(casted),
                        Exp.mapBin(db.SUPERNODE_EDGE_PROPERTIES_BIN), CTX.mapKey(Value.get(vertexIdKeyHashString)),
                        CTX.mapKey(Value.get(propertyKey)));
            } else if (predicate.getBiPredicate().equals(Compare.lte)) {
                return MapExp.getByValueRange(MapReturnType.EXISTS, Exp.val(Long.MIN_VALUE), Exp.val(casted + 1),
                        Exp.mapBin(db.SUPERNODE_EDGE_PROPERTIES_BIN), CTX.mapKey(Value.get(vertexIdKeyHashString)),
                        CTX.mapKey(Value.get(propertyKey)));
            } else if (predicate.getBiPredicate().equals(Compare.gt)) {
                return MapExp.getByValueRange(MapReturnType.EXISTS, Exp.val(casted + 1), Exp.val(Long.MAX_VALUE),
                        Exp.mapBin(db.SUPERNODE_EDGE_PROPERTIES_BIN), CTX.mapKey(Value.get(vertexIdKeyHashString)),
                        CTX.mapKey(Value.get(propertyKey)));
            } else if (predicate.getBiPredicate().equals(Compare.gte)) {
                return MapExp.getByValueRange(MapReturnType.EXISTS, Exp.val(casted), Exp.val(Long.MAX_VALUE),
                        Exp.mapBin(db.SUPERNODE_EDGE_PROPERTIES_BIN), CTX.mapKey(Value.get(vertexIdKeyHashString)),
                        CTX.mapKey(Value.get(propertyKey)));
            } else {
                throw new RuntimeException(String.format("%s not a supported predicate", predicate));
            }
        } else {
            // If the accessor is an ID use a special expression that leverages the label key instead.
            if (T.id.getAccessor().equals(propertyKey)) {
                final FireflyPhatEdgeId edgeId = (FireflyPhatEdgeId) db.getIdFactory().createId(value, FireflyEdge.class);
                return MapExp.getByKey(MapReturnType.EXISTS, Exp.Type.BOOL, Exp.val(edgeId.getUniqueId()),
                        Exp.mapBin(db.SUPERNODE_EDGE_PROPERTIES_BIN), CTX.mapKey(Value.get(vertexIdKeyHashString)),
                        CTX.mapKey(Value.get(EDGE_SUPERNODE_LABEL_KEY)));
            } else {
                return MapExp.getByValue(MapReturnType.EXISTS, Exp.val((String) value),
                        Exp.mapBin(db.SUPERNODE_EDGE_PROPERTIES_BIN), CTX.mapKey(Value.get(vertexIdKeyHashString)),
                        CTX.mapKey(Value.get(propertyKey)));
            }
        }
    }

    private static Exp phatEdgeCompoundPredicateToExp(final AerospikeConnection db, final String vertexIdKeyHashString,
                                                      final Map.Entry<String, List<Long>> compoundHasContainer) {
        return MapExp.getByValueRange(MapReturnType.EXISTS, Exp.val(compoundHasContainer.getValue().get(0)),
                Exp.val(compoundHasContainer.getValue().get(1)), Exp.mapBin(db.SUPERNODE_EDGE_PROPERTIES_BIN),
                CTX.mapKey(Value.get(vertexIdKeyHashString)), CTX.mapKey(Value.get(compoundHasContainer.getKey())));
    }

    private static class PhatEdgeHasContainers {
        private final Map<String, Boolean> compoundContainerKeys = new HashMap<>();
        private final List<HasContainer> filteredHasContainers = new ArrayList<>();
        private final Map<String, List<Long>> compoundHasContainers = new HashMap<>();

        private PhatEdgeHasContainers(final List<HasContainer> hasContainers) {
            // First pass - mark HasContainer keys that can be compounded.
            findCompoundKeys(hasContainers);
            // Second pass - separate hasContainers into types.
            separateHasContainers(hasContainers);
        }

        private void findCompoundKeys(final List<HasContainer> hasContainers) {
            for (final HasContainer hasContainer : hasContainers) {
                final P<?> predicate = hasContainer.getPredicate();
                final Object value = predicate.getValue();
                // Update !equals(Compare.eq) if new predicates that can't be compounded are supported in the future.
                if (!predicate.getBiPredicate().equals(Compare.eq) &&
                        (Long.class.isAssignableFrom(value.getClass()) ||
                                Integer.class.isAssignableFrom(value.getClass()))) {
                    final String key = hasContainer.getKey();
                    // A HasContainer can only be compounded if there are two or more. Add the key the first time it's
                    // found; mark the key as true for any successive times.
                    if (compoundContainerKeys.containsKey(key)) {
                        compoundContainerKeys.put(key, true);
                    } else {
                        compoundContainerKeys.put(key, false);
                    }
                }
            }
        }

        private void separateHasContainers(final List<HasContainer> hasContainers) {
            // Initialize compoundHasContainers.
            for (final Map.Entry<String, Boolean> compoundContainerKey : compoundContainerKeys.entrySet()) {
                if (compoundContainerKey.getValue()) {
                    final List<Long> defaultMinMax = new ArrayList<>();
                    defaultMinMax.add(Long.MIN_VALUE);
                    defaultMinMax.add(Long.MAX_VALUE);
                    compoundHasContainers.put(compoundContainerKey.getKey(), defaultMinMax);
                }
            }
            // Filter the hasContainers.
            for (final HasContainer hasContainer : hasContainers) {
                final String key = hasContainer.getKey();
                final P<?> predicate = hasContainer.getPredicate();
                if (compoundHasContainers.containsKey(key) &&
                        !predicate.getBiPredicate().equals(Compare.eq)) {
                    final Long value = ((Number) predicate.getValue()).longValue();
                    final List<Long> lowerUpperBounds = compoundHasContainers.get(key);
                    // lower is inclusive; upper is exclusive. Handle it here so no need to mess with off-by-1 issues later.
                    if (predicate.getBiPredicate().equals(Compare.lt)) {
                        if (value < lowerUpperBounds.get(1)) {
                            lowerUpperBounds.set(1, value);
                        }
                    } else if (predicate.getBiPredicate().equals(Compare.lte)) {
                        if (value + 1 < lowerUpperBounds.get(1)) {
                            lowerUpperBounds.set(1, value + 1);
                        }
                    } else if (predicate.getBiPredicate().equals(Compare.gt)) {
                        if (value + 1 > lowerUpperBounds.get(0)) {
                            lowerUpperBounds.set(0, value + 1);
                        }
                    } else if (predicate.getBiPredicate().equals(Compare.gte)) {
                        if (value > lowerUpperBounds.get(0)) {
                            lowerUpperBounds.set(0, value);
                        }
                    } else {
                        // This should never happen.
                        throw new RuntimeException(String.format("%s not a supported predicate for setting a lower/upper bound", predicate));
                    }
                } else {
                    filteredHasContainers.add(hasContainer);
                }
            }
        }
    }
}
