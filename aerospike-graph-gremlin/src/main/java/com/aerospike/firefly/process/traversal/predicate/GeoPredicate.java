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

package com.aerospike.firefly.process.traversal.predicate;

import com.aerospike.firefly.util.FireflyGeoValue;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Internal server-side predicate for geospatial vertex property queries.
 * Never serialized to clients; produced by {@link com.aerospike.firefly.process.traversal.strategy.optimization.FireflyGeoPredicateStrategy}.
 */
public final class GeoPredicate implements PBiPredicate<Object, Object>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final double EARTH_RADIUS_METERS = 6371000.0;

    public enum QueryType {
        WITHIN_RADIUS,
        WITHIN_REGION,
        EXACT_POINT
    }

    private final QueryType queryType;
    private final double centerLon;
    private final double centerLat;
    private final double radiusMeters;
    private final List<List<Double>> regionRing;
    private final String regionGeoJson;
    private final String exactGeoJson;

    private GeoPredicate(final QueryType queryType,
                           final double centerLon,
                           final double centerLat,
                           final double radiusMeters,
                           final List<List<Double>> regionRing,
                           final String regionGeoJson,
                           final String exactGeoJson) {
        this.queryType = queryType;
        this.centerLon = centerLon;
        this.centerLat = centerLat;
        this.radiusMeters = radiusMeters;
        this.regionRing = regionRing;
        this.regionGeoJson = regionGeoJson;
        this.exactGeoJson = exactGeoJson;
    }

    public static P<Object> withinRadius(final double lon, final double lat, final double radiusMeters) {
        FireflyGeoValue.validateCoordinates(lon, lat);
        if (radiusMeters < 0) {
            throw new IllegalArgumentException("Geo radius must be non-negative.");
        }
        final GeoPredicate predicate = new GeoPredicate(QueryType.WITHIN_RADIUS, lon, lat, radiusMeters, null, null, null);
        return new P<>((PBiPredicate<Object, Object>) predicate, predicate);
    }

    public static P<Object> withinRegion(final List<List<Double>> ring) {
        final String regionGeoJson = FireflyGeoValue.toGeoJsonPolygon(ring);
        final GeoPredicate predicate = new GeoPredicate(QueryType.WITHIN_REGION, 0, 0, 0, ring, regionGeoJson, null);
        return new P<>((PBiPredicate<Object, Object>) predicate, predicate);
    }

    public static P<Object> exactPoint(final List<?> coordinatePair) {
        final double[] coords = FireflyGeoValue.parseCoordinatePair(coordinatePair);
        final String exactGeoJson = FireflyGeoValue.toGeoJsonPoint(coords[0], coords[1]);
        final GeoPredicate predicate = new GeoPredicate(QueryType.EXACT_POINT, coords[0], coords[1], 0, null, null, exactGeoJson);
        return new P<>((PBiPredicate<Object, Object>) predicate, predicate);
    }

    public static GeoPredicate unwrap(final P<?> predicate) {
        if (predicate == null) {
            return null;
        }
        if (predicate.getBiPredicate() instanceof GeoPredicate) {
            return (GeoPredicate) predicate.getBiPredicate();
        }
        if (predicate.getValue() instanceof GeoPredicate) {
            return (GeoPredicate) predicate.getValue();
        }
        return null;
    }

    public QueryType getQueryType() {
        return queryType;
    }

    public double getCenterLon() {
        return centerLon;
    }

    public double getCenterLat() {
        return centerLat;
    }

    public double getRadiusMeters() {
        return radiusMeters;
    }

    public String getRegionGeoJson() {
        return regionGeoJson;
    }

    public String getExactGeoJson() {
        return exactGeoJson;
    }

    @Override
    public boolean test(final Object key, final Object value) {
        final Object storedValue = value instanceof GeoPredicate ? key : value;
        if (storedValue == null) {
            return false;
        }
        final List<String> points = extractGeoJsonPoints(storedValue);
        for (final String point : points) {
            if (matchesPoint(point)) {
                return true;
            }
        }
        return false;
    }

    private List<String> extractGeoJsonPoints(final Object value) {
        final List<String> points = new ArrayList<>();
        if (value instanceof String) {
            points.add((String) value);
        } else if (value instanceof List) {
            final List<?> list = (List<?>) value;
            if (FireflyGeoValue.isCoordinatePair(list)) {
                final double[] coords = FireflyGeoValue.parseCoordinatePair(list);
                points.add(FireflyGeoValue.toGeoJsonPoint(coords[0], coords[1]));
            } else {
                for (final Object element : list) {
                    if (element instanceof String) {
                        points.add((String) element);
                    } else if (element instanceof List) {
                        final List<Double> coords = FireflyGeoValue.fromGeoJsonPoint(
                                FireflyGeoValue.toGeoJsonPoint(
                                        ((Number) ((List<?>) element).get(0)).doubleValue(),
                                        ((Number) ((List<?>) element).get(1)).doubleValue()));
                        points.add(FireflyGeoValue.toGeoJsonPoint(coords.get(0), coords.get(1)));
                    }
                }
            }
        } else if (value instanceof Collection) {
            for (final Object element : (Collection<?>) value) {
                points.addAll(extractGeoJsonPoints(element));
            }
        }
        return points;
    }

    private boolean matchesPoint(final String geoJsonPoint) {
        final List<Double> coords = FireflyGeoValue.fromGeoJsonPoint(geoJsonPoint);
        final double lon = coords.get(0);
        final double lat = coords.get(1);
        switch (queryType) {
            case WITHIN_RADIUS:
                return haversineMeters(centerLon, centerLat, lon, lat) <= radiusMeters;
            case WITHIN_REGION:
                return pointInRing(lon, lat, regionRing);
            case EXACT_POINT:
                return exactGeoJson.equals(geoJsonPoint);
            default:
                return false;
        }
    }

    static double haversineMeters(final double lon1, final double lat1, final double lon2, final double lat2) {
        final double dLat = Math.toRadians(lat2 - lat1);
        final double dLon = Math.toRadians(lon2 - lon1);
        final double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        final double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return EARTH_RADIUS_METERS * c;
    }

    static boolean pointInRing(final double lon, final double lat, final List<List<Double>> ring) {
        boolean inside = false;
        for (int i = 0, j = ring.size() - 1; i < ring.size(); j = i++) {
            final double xi = ring.get(i).get(0);
            final double yi = ring.get(i).get(1);
            final double xj = ring.get(j).get(0);
            final double yj = ring.get(j).get(1);
            final boolean intersect = ((yi > lat) != (yj > lat))
                    && (lon < (xj - xi) * (lat - yi) / (yj - yi + 0.0) + xi);
            if (intersect) {
                inside = !inside;
            }
        }
        return inside;
    }

    @Override
    public String toString() {
        return "GeoPredicate{" + queryType + '}';
    }
}
