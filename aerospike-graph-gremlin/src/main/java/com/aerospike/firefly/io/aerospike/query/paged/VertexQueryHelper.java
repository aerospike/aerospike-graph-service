/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.io.aerospike.query.paged;

import com.aerospike.client.Value;
import com.aerospike.client.cdt.CTX;
import com.aerospike.client.cdt.MapReturnType;
import com.aerospike.client.exp.Exp;
import com.aerospike.client.exp.Expression;
import com.aerospike.client.exp.MapExp;
import com.aerospike.client.query.Filter;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.aerospike.firefly.io.FireflyIndexMetadata;
import com.aerospike.firefly.process.traversal.predicate.GeoPredicate;
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

public class VertexQueryHelper {

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

    protected static Exp getValue(final Object value) {
        if (value == null) {
            return Exp.nil();
        }
        if (Boolean.class.isAssignableFrom(value.getClass())) {
            // Booleans are stored as a byte array of [1] or [0].
            return Exp.val((Boolean) value ? new byte[]{1} : new byte[]{0});
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
        if (IndexType.GEO2DSPHERE.equals(indexInfo.indexType)) {
            final GeoPredicate geoPredicate = GeoPredicate.unwrap(predicate);
            if (geoPredicate == null) {
                throw new IllegalArgumentException("Expected GeoPredicate for geo index query.");
            }
            final Long schemaKey = db.schemaManager.getGeoPropertyRead(indexInfo.key);
            final CTX[] ctx = new CTX[]{CTX.mapKey(Value.get(schemaKey))};
            if (geoPredicate.getQueryType() == GeoPredicate.QueryType.WITHIN_RADIUS) {
                return Filter.geoWithinRadius(db.getConfig().geoDataBin, IndexCollectionType.LIST,
                        geoPredicate.getCenterLon(), geoPredicate.getCenterLat(), geoPredicate.getRadiusMeters(), ctx);
            } else if (geoPredicate.getQueryType() == GeoPredicate.QueryType.WITHIN_REGION) {
                return Filter.geoWithinRegion(db.getConfig().geoDataBin, IndexCollectionType.LIST,
                        geoPredicate.getRegionGeoJson(), ctx);
            }
            throw new IllegalArgumentException("Geo index queries require radius or region predicates.");
        }
        final String name;
        if (db.getConfig().labelBin.equals(indexInfo.key)) {
            name = db.getConfig().labelBin;
        } else if (indexInfo.setName.equals(db.getConfig().vertexAeroSet)) {
            name = db.getConfig().vertexPropertyDataBin;
        } else {
            throw new IllegalArgumentException(
                    "Cannot create filter for index with unknown set name: " + indexInfo.setName + " and key " + indexInfo.key);
        }

        final IndexCollectionType type = db.getConfig().labelBin.equals(indexInfo.key) ?
                IndexCollectionType.DEFAULT : IndexCollectionType.MAPKEYS;
        final Object value = predicate.getValue();
        if (db.getConfig().labelBin.equals(indexInfo.key)) {
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
        final GeoPredicate geoPredicate = GeoPredicate.unwrap(predicate);
        if (geoPredicate != null && db.getConfig().geoDataBin.equals(binName)) {
            return geoPredicateToExpression(db, mapKey, geoPredicate);
        }
        if (geoPredicate != null && db.schemaManager.isRegisteredGeoProperty(mapKey)) {
            return geoPredicateToExpression(db, mapKey, geoPredicate);
        }
        // If the bin is the label bin, we can make a very simple predicate.
        if (db.getConfig().labelBin.equals(binName)) {
            return Exp.eq(Exp.intBin(db.getConfig().labelBin), Exp.val(db.schemaManager.getVertexLabelRead((String) predicate.getValue())));
        }

        if (predicate.getBiPredicate().equals(Compare.eq)) {
            return MapExp.getByKey(MapReturnType.EXISTS,
                    Exp.Type.BOOL,
                    getValue(predicate.getValue()),
                    Exp.mapBin(binName),
                    CTX.mapKey(Value.get(db.schemaManager.getVertexPropertyRead(mapKey))));
        } else if (predicate.getBiPredicate().equals(Compare.neq)) {
            return Exp.not(MapExp.getByKey(MapReturnType.EXISTS,
                    Exp.Type.BOOL,
                    getValue(predicate.getValue()),
                    Exp.mapBin(binName),
                    CTX.mapKey(Value.get(db.schemaManager.getVertexPropertyRead(mapKey)))));
        } else if (predicate.getBiPredicate().equals(Compare.lt)) {
            return MapExp.getByKeyRange(MapReturnType.EXISTS,
                    getValue(Long.MIN_VALUE),
                    getValue(castLong(predicate.getValue())),
                    Exp.mapBin(binName),
                    CTX.mapKey(Value.get(db.schemaManager.getVertexPropertyRead(mapKey))));
        } else if (predicate.getBiPredicate().equals(Compare.lte)) {
            final Long value = castLong(predicate.getValue()) + 1;
            return MapExp.getByKeyRange(MapReturnType.EXISTS,
                    getValue(Long.MIN_VALUE),
                    getValue(value),
                    Exp.mapBin(binName),
                    CTX.mapKey(Value.get(db.schemaManager.getVertexPropertyRead(mapKey))));
        } else if (predicate.getBiPredicate().equals(Compare.gt)) {
            final Long value = castLong(predicate.getValue()) - 1;
            return MapExp.getByKeyRange(MapReturnType.EXISTS,
                    getValue(value),
                    getValue(Long.MAX_VALUE),
                    Exp.mapBin(binName),
                    CTX.mapKey(Value.get(db.schemaManager.getVertexPropertyRead(mapKey))));
        } else if (predicate.getBiPredicate().equals(Compare.gte)) {
            return MapExp.getByKeyRange(MapReturnType.EXISTS,
                    getValue(predicate.getValue()),
                    getValue(Long.MAX_VALUE),
                    Exp.mapBin(binName),
                    CTX.mapKey(Value.get(db.schemaManager.getVertexPropertyRead(mapKey))));
        } else {
            // This should never happen.
            throw new IllegalArgumentException("Unsupported predicate provided for Vertex query. Please contact support. Predicate: " + predicate.getBiPredicate());
        }
    }

    public static Expression hasContainerListToExpression(final AerospikeConnection db, final List<HasContainer> hasContainers) {
        if (hasContainers.isEmpty() && !db.getConfig().ttlEnabledFlag) {
            return null;
        }
        final List<Exp> exps = new ArrayList<>();
        if (db.getConfig().ttlEnabledFlag) {
            final Exp ttlExp = getVertexTtlExp(db);
            exps.add(ttlExp);
        }

        for (final HasContainer h : hasContainers) {
            final String binName = resolvePredicateBinName(db, h.getKey());
            final Exp expFromPredicate = predicateToExpression(db, binName, h.getKey(), h.getPredicate());
            exps.add(expFromPredicate);
        }
        return exps.size() == 1 ? Exp.build(exps.get(0)) : Exp.build(Exp.and(exps.toArray(new Exp[0])));
    }

    private static String resolvePredicateBinName(final AerospikeConnection db, final String propertyKey) {
        if ("~label".equals(propertyKey)) {
            return db.getConfig().labelBin;
        }
        if (db.schemaManager.isRegisteredGeoProperty(propertyKey)) {
            return db.getConfig().geoDataBin;
        }
        return db.getConfig().vertexPropertyDataBin;
    }

    private static Exp geoPredicateToExpression(final AerospikeConnection db,
                                                  final String mapKey,
                                                  final GeoPredicate geoPredicate) {
        final Long schemaKey = db.schemaManager.getGeoPropertyRead(mapKey);
        final Exp geoValues = MapExp.getByKey(MapReturnType.VALUE, Exp.Type.LIST,
                Exp.val(schemaKey), Exp.mapBin(db.getConfig().geoDataBin));
        switch (geoPredicate.getQueryType()) {
            case WITHIN_RADIUS:
                final String aeroCircle = String.format(
                        "{\"type\":\"AeroCircle\",\"coordinates\":[%.10f,%.10f],\"radius\":%.3f}",
                        geoPredicate.getCenterLon(), geoPredicate.getCenterLat(), geoPredicate.getRadiusMeters());
                return Exp.geoCompare(geoValues, Exp.geo(aeroCircle));
            case WITHIN_REGION:
                return Exp.geoCompare(geoValues, Exp.geo(geoPredicate.getRegionGeoJson()));
            case EXACT_POINT:
                return Exp.eq(geoValues, Exp.val(List.of(geoPredicate.getExactGeoJson())));
            default:
                throw new IllegalArgumentException("Unsupported geo predicate: " + geoPredicate.getQueryType());
        }
    }

    public static Exp[] hasContainerListToExpArray(final AerospikeConnection db, final List<HasContainer> hasContainers, final Class<? extends FireflyElement> clazz) {
        // If the key is ~label, then bin name is label, else it depends on whether this is vertex or edge.
        if (!FireflyVertex.class.isAssignableFrom(clazz)) {
            throw new IllegalArgumentException("Cannot push predicates down to: " + clazz);
        }
        final List<Exp> exps = new ArrayList<>();
        if (db.getConfig().ttlEnabledFlag) {
            final Exp ttlExp = getVertexTtlExp(db);
            exps.add(ttlExp);
        }
        for (final HasContainer h : hasContainers) {
            final Exp expFromPredicate = predicateToExpression(db,
                    resolvePredicateBinName(db, h.getKey()),
                    h.getKey(), h.getPredicate());
            exps.add(expFromPredicate);
        }
        return exps.toArray(new Exp[0]);
    }

    private static Exp getVertexTtlExp(final AerospikeConnection db) {
        return Exp.or(
                Exp.gt(Exp.intBin(db.getConfig().ttlBin), Exp.val(System.currentTimeMillis())),
                Exp.not(
                        Exp.binExists(db.getConfig().ttlBin)
                )
        );
    }
}
