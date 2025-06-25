package com.aerospike.firefly.io.aerospike.query.paged;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.MapExp;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.id.FireflyId;
import com.aerospike.firefly.structure.id.FireflyPhatEdgeId;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Compare;
import org.apache.tinkerpop.gremlin.process.traversal.Contains;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_ADJACENT_ID_KEY;
import static com.aerospike.firefly.structure.FireflyEdge.EDGE_SUPERNODE_LABEL_KEY;

public class EdgeQueryHelper {

    public static Expression phatEdgeHasContainerListToExpression(final AerospikeConnection db,
                                                                  final List<HasContainer> hasContainers,
                                                                  final Set<String> labels,
                                                                  final FireflyId vertexId,
                                                                  final FireflyId adjacentVertexId,
                                                                  final Direction direction) {
        final Exp labelExp = getPhatEdgeLabelExp(db, labels, direction, vertexId);
        final Exp propertiesExp = getPhatEdgePropertyExp(db, hasContainers, direction, vertexId);
        final Exp adjacentVertexExp = getPhatEdgeAdjacentVertexExp(db, adjacentVertexId, direction, vertexId);
        if (labelExp == null && propertiesExp == null && adjacentVertexExp == null) {
            return null;
        }
        Exp expToBuild = Exp.val(true);
        if (labelExp != null) {
            expToBuild = Exp.and(expToBuild, labelExp);
        }
        if (propertiesExp != null) {
            expToBuild = Exp.and(expToBuild, propertiesExp);
        }
        if (adjacentVertexExp != null) {
            expToBuild = Exp.and(expToBuild, adjacentVertexExp);
        }
        return Exp.build(expToBuild);
    }

    private static Exp getPhatEdgeLabelExp(final AerospikeConnection db,
                                           final Set<String> labels,
                                           final Direction direction,
                                           final FireflyId vertexId) {
        if (labels.isEmpty()) {
            return null;
        } else {
            final String binName = direction == Direction.OUT ? db.SUPERNODES_OUT_BIN : db.SUPERNODES_IN_BIN;
            final List<Exp> allLabelExp = new ArrayList<>();
            for (final String label : labels) {
                final Long schemaLabelKey = db.schemaManager.getEdgePropertyRead(EDGE_SUPERNODE_LABEL_KEY);
                final Long schemaLabel = db.schemaManager.getEdgeLabelRead(label);
                final Exp labelExp = MapExp.getByValue(MapReturnType.EXISTS, Exp.val(schemaLabel),
                        Exp.mapBin(binName), CTX.mapKey(Value.get(vertexId.getKeyHashString())),
                        CTX.mapKey(Value.get(schemaLabelKey)));
                allLabelExp.add(labelExp);
            }
            if (allLabelExp.size() == 1) {
                return allLabelExp.get(0);
            } else {
                return Exp.or(allLabelExp.toArray(new Exp[0]));
            }
        }
    }

    private static Exp getPhatEdgeAdjacentVertexExp(final AerospikeConnection db,
                                                    final FireflyId adjacentVertexId,
                                                    final Direction direction,
                                                    final FireflyId vertexId) {
        if (adjacentVertexId == null) {
            return null;
        }
        final Long schemaAdjacentIdKey = db.schemaManager.getEdgePropertyRead(EDGE_SUPERNODE_ADJACENT_ID_KEY);
        final String binName;
        if (direction == Direction.IN) {
            binName = db.SUPERNODES_IN_BIN;
        } else if (direction == Direction.OUT) {
            binName = db.SUPERNODES_OUT_BIN;
        } else {
            // This should never happen.
            throw new IllegalArgumentException("Adjacency pushdown filter for adjacent Vertex ID can not be invoked with Direction BOTH.");
        }
        final Exp vertexUserIdExp = adjacentVertexId.getUserId() instanceof String ?
                Exp.val((String) adjacentVertexId.getUserId()) :
                Exp.val(((Number) adjacentVertexId.getUserId()).longValue());
        return MapExp.getByValue(MapReturnType.EXISTS, vertexUserIdExp,
                Exp.mapBin(binName), CTX.mapKey(Value.get(vertexId.getKeyHashString())),
                CTX.mapKey(Value.get(schemaAdjacentIdKey)));
    }

    private static Exp getPhatEdgePropertyExp(final AerospikeConnection db,
                                              final List<HasContainer> hasContainers,
                                              final Direction direction,
                                              final FireflyId vertexId) {
        if (hasContainers.isEmpty()) {
            return null;
        }
        final PhatEdgeHasContainers phatEdgeHasContainers = new PhatEdgeHasContainers(hasContainers);
        final Exp[] exps = phatEdgeHasContainers.filteredHasContainers.stream().map(hasContainer ->
                        phatEdgePredicateToExp(db, direction, vertexId, hasContainer.getKey(), hasContainer.getPredicate()))
                .toArray(Exp[]::new);
        final Exp[] compoundExps = phatEdgeHasContainers.compoundHasContainers.entrySet().stream().map(compoundContainer ->
                        phatEdgeCompoundPredicateToExp(db, direction, vertexId, compoundContainer))
                .toArray(Exp[]::new);
        final Exp[] allExps = ArrayUtils.addAll(exps, compoundExps);
        if (allExps.length == 1) {
            return allExps[0];
        } else {
            return Exp.and(allExps);
        }
    }

    private static Exp phatEdgePredicateToExp(final AerospikeConnection db, final Direction direction,
                                              final FireflyId vertexId, final String propertyKey,
                                              final P<?> predicate) {
        // Build expression for nested Phat Edge properties.
        final String binName = direction == Direction.OUT ? db.SUPERNODES_OUT_BIN : db.SUPERNODES_IN_BIN;
        final Long schemaPropertyKey = db.schemaManager.getEdgePropertyRead(propertyKey);
        final Object value = predicate.getValue();
        if (predicate.getBiPredicate().equals(Contains.within)) {
            final Exp[] containsExps = ((Collection<?>) value).stream().map(collectionValue ->
                            phatEdgePredicateToExp(db, direction, vertexId, propertyKey, new P<>(Compare.eq, collectionValue)))
                    .toArray(Exp[]::new);
            return containsExps.length == 1 ? containsExps[0] : Exp.or(containsExps);
        } else if (Number.class.isAssignableFrom(value.getClass())) {
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
                        Exp.mapBin(binName), CTX.mapKey(Value.get(vertexId.getKeyHashString())),
                        CTX.mapKey(Value.get(schemaPropertyKey)));
            } else if (predicate.getBiPredicate().equals(Compare.lt)) {
                // getByValueRange valueBegin is inclusive; valueEnd is exclusive.
                return MapExp.getByValueRange(MapReturnType.EXISTS, Exp.val(Long.MIN_VALUE), Exp.val(casted),
                        Exp.mapBin(binName), CTX.mapKey(Value.get(vertexId.getKeyHashString())),
                        CTX.mapKey(Value.get(schemaPropertyKey)));
            } else if (predicate.getBiPredicate().equals(Compare.lte)) {
                return MapExp.getByValueRange(MapReturnType.EXISTS, Exp.val(Long.MIN_VALUE), Exp.val(casted + 1),
                        Exp.mapBin(binName), CTX.mapKey(Value.get(vertexId.getKeyHashString())),
                        CTX.mapKey(Value.get(schemaPropertyKey)));
            } else if (predicate.getBiPredicate().equals(Compare.gt)) {
                return MapExp.getByValueRange(MapReturnType.EXISTS, Exp.val(casted + 1), Exp.val(Long.MAX_VALUE),
                        Exp.mapBin(binName), CTX.mapKey(Value.get(vertexId.getKeyHashString())),
                        CTX.mapKey(Value.get(schemaPropertyKey)));
            } else if (predicate.getBiPredicate().equals(Compare.gte)) {
                return MapExp.getByValueRange(MapReturnType.EXISTS, Exp.val(casted), Exp.val(Long.MAX_VALUE),
                        Exp.mapBin(binName), CTX.mapKey(Value.get(vertexId.getKeyHashString())),
                        CTX.mapKey(Value.get(schemaPropertyKey)));
            } else {
                throw new RuntimeException(String.format("%s not a supported predicate", predicate));
            }
        } else {
            // If the accessor is an ID use a special expression that leverages the label key instead.
            if (T.id.getAccessor().equals(propertyKey)) {
                final FireflyPhatEdgeId edgeId = db.getIdFactory().createEdgeId(value);
                final Long schemaLabelKey = db.schemaManager.getEdgePropertyRead(EDGE_SUPERNODE_LABEL_KEY);
                return MapExp.getByKey(MapReturnType.EXISTS, Exp.Type.BOOL, Exp.val(edgeId.getUniqueId()),
                        Exp.mapBin(binName), CTX.mapKey(Value.get(vertexId.getKeyHashString())),
                        CTX.mapKey(Value.get(schemaLabelKey)));
            } else {
                return MapExp.getByValue(MapReturnType.EXISTS, Exp.val((String) value),
                        Exp.mapBin(binName), CTX.mapKey(Value.get(vertexId.getKeyHashString())),
                        CTX.mapKey(Value.get(schemaPropertyKey)));
            }
        }
    }

    private static Exp phatEdgeCompoundPredicateToExp(final AerospikeConnection db, final Direction direction,
                                                      final FireflyId vertexId,
                                                      final Map.Entry<String, List<Long>> compoundHasContainer) {
        final String binName = direction == Direction.OUT ? db.SUPERNODES_OUT_BIN : db.SUPERNODES_IN_BIN;
        final Long schemaCompoundPropertyKey = db.schemaManager.getEdgePropertyRead(compoundHasContainer.getKey());
        return MapExp.getByValueRange(MapReturnType.EXISTS, Exp.val(compoundHasContainer.getValue().get(0)),
                Exp.val(compoundHasContainer.getValue().get(1)), Exp.mapBin(binName),
                CTX.mapKey(Value.get(vertexId.getKeyHashString())), CTX.mapKey(Value.get(schemaCompoundPropertyKey)));
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
