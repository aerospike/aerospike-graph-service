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

package com.aerospike.firefly.util;

import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class FireflyGeoValue {
    public static final long GEO_TYPE_HINT = 10L;

    private FireflyGeoValue() {
    }

    public static boolean isGeoPropertyKey(final String key, final boolean geoEnabled, final Collection<String> explicitGeoKeys) {
        if (!geoEnabled || key == null || key.isEmpty()) {
            return false;
        }
        if (explicitGeoKeys != null && explicitGeoKeys.contains(key)) {
            return true;
        }
        return key.startsWith("geo");
    }

    public static boolean isGeoCoordinateInput(final Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof List) {
            final List<?> list = (List<?>) value;
            if (list.isEmpty()) {
                return false;
            }
            if (isCoordinatePair(list)) {
                return true;
            }
            for (final Object element : list) {
                if (!(element instanceof List) || !isCoordinatePair((List<?>) element)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    public static boolean isCoordinatePair(final List<?> list) {
        if (list.size() != 2) {
            return false;
        }
        return isNumeric(list.get(0)) && isNumeric(list.get(1));
    }

    private static boolean isNumeric(final Object value) {
        return value instanceof Number;
    }

    public static double[] parseCoordinatePair(final List<?> list) {
        if (!isCoordinatePair(list)) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(list);
        }
        final double lon = ((Number) list.get(0)).doubleValue();
        final double lat = ((Number) list.get(1)).doubleValue();
        validateCoordinates(lon, lat);
        return new double[]{lon, lat};
    }

    public static void validateCoordinates(final double lon, final double lat) {
        if (lon < -180.0 || lon > 180.0 || lat < -90.0 || lat > 90.0) {
            throw new IllegalArgumentException(String.format(
                    "Invalid geo coordinates [%.6f, %.6f]: longitude must be in [-180, 180] and latitude in [-90, 90].",
                    lon, lat));
        }
    }

    public static String toGeoJsonPoint(final double lon, final double lat) {
        validateCoordinates(lon, lat);
        return String.format("{\"type\":\"Point\",\"coordinates\":[%.10f,%.10f]}", lon, lat);
    }

    public static List<String> toGeoJsonPoints(final Object value) {
        if (!(value instanceof List)) {
            throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
        }
        final List<?> list = (List<?>) value;
        if (list.isEmpty()) {
            throw new IllegalArgumentException("Geo coordinate list cannot be empty.");
        }
        final List<String> points = new ArrayList<>();
        if (isCoordinatePair(list)) {
            final double[] coords = parseCoordinatePair(list);
            points.add(toGeoJsonPoint(coords[0], coords[1]));
            return points;
        }
        for (final Object element : list) {
            if (!(element instanceof List)) {
                throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
            }
            final double[] coords = parseCoordinatePair((List<?>) element);
            points.add(toGeoJsonPoint(coords[0], coords[1]));
        }
        return points;
    }

    public static List<Double> fromGeoJsonPoint(final String geoJson) {
        if (geoJson == null || geoJson.isBlank()) {
            throw new IllegalArgumentException("GeoJSON point value cannot be null or empty.");
        }
        final int coordinatesIndex = geoJson.indexOf("\"coordinates\"");
        if (coordinatesIndex < 0) {
            throw new IllegalArgumentException("Invalid GeoJSON point: " + geoJson);
        }
        final int openBracket = geoJson.indexOf('[', coordinatesIndex);
        final int closeBracket = geoJson.indexOf(']', openBracket);
        if (openBracket < 0 || closeBracket < 0) {
            throw new IllegalArgumentException("Invalid GeoJSON point: " + geoJson);
        }
        final String coordinateSection = geoJson.substring(openBracket + 1, closeBracket);
        final String[] parts = coordinateSection.split(",");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid GeoJSON point coordinates: " + geoJson);
        }
        final double lon = Double.parseDouble(parts[0].trim());
        final double lat = Double.parseDouble(parts[1].trim());
        validateCoordinates(lon, lat);
        final List<Double> result = new ArrayList<>(2);
        result.add(lon);
        result.add(lat);
        return result;
    }

    public static Object toClientValue(final Object storedValue) {
        if (storedValue instanceof String) {
            return fromGeoJsonPoint((String) storedValue);
        }
        if (storedValue instanceof List) {
            final List<?> storedList = (List<?>) storedValue;
            if (storedList.size() == 1 && storedList.get(0) instanceof String) {
                return fromGeoJsonPoint((String) storedList.get(0));
            }
            final List<List<Double>> points = new ArrayList<>();
            for (final Object element : storedList) {
                if (element instanceof String) {
                    points.add(fromGeoJsonPoint((String) element));
                } else {
                    throw new IllegalArgumentException("Unexpected geo stored value element: " + element);
                }
            }
            return points.size() == 1 ? points.get(0) : points;
        }
        throw new IllegalArgumentException("Unexpected geo stored value: " + storedValue);
    }

    public static String toGeoJsonPolygon(final List<List<Double>> ring) {
        if (ring == null || ring.size() < 4) {
            throw new IllegalArgumentException("Geo region must contain at least four coordinate pairs.");
        }
        final StringBuilder builder = new StringBuilder("{\"type\":\"Polygon\",\"coordinates\":[[");
        for (int i = 0; i < ring.size(); i++) {
            final List<Double> point = ring.get(i);
            if (point.size() != 2) {
                throw new IllegalArgumentException("Geo region coordinate pair must contain exactly two numbers.");
            }
            validateCoordinates(point.get(0), point.get(1));
            if (i > 0) {
                builder.append(',');
            }
            builder.append('[').append(point.get(0)).append(',').append(point.get(1)).append(']');
        }
        builder.append("]]}");
        return builder.toString();
    }
}
