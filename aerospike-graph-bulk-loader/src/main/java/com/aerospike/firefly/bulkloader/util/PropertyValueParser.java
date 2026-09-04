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

package com.aerospike.firefly.bulkloader.util;

import com.aerospike.firefly.process.call.bulkload.utils.exception.BadCsvEntryException;
import com.aerospike.firefly.util.FireflyGeoValue;

import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class PropertyValueParser {
    private final String nullValue;

    public PropertyValueParser(final String nullValue) {
        this.nullValue = nullValue;
    }

    public Long parseLong(final String value) {
        if (value.equals(this.nullValue)) {
            return null;
        }

        return Long.parseLong(value);
    }

    public Integer parseInt(final String value) {
        if (value.equals(this.nullValue)) {
            return null;
        }

        return Integer.parseInt(value);
    }

    public Double parseDouble(final String value) {
        if (value.equals(this.nullValue)) {
            return null;
        }

        return Double.parseDouble(value);
    }

    public Boolean parseBoolean(final String value) {
        if (value.equals(this.nullValue)) {
            return null;
        }
        final String sanitizedBooleanString = value.strip().toLowerCase();
        if ("true".equals(sanitizedBooleanString)) {
            return true;
        } else if ("false".equals(sanitizedBooleanString)) {
            return false;
        } else {
            throw new RuntimeException("Could not parse the value '" + value + "' into a boolean.");
        }
    }

    public String parseString(final String value) {
        if (value.equals(this.nullValue)) {
            return null;
        }

        return value;
    }

    public Date parseDate(final String value) {
        if (value.equals(this.nullValue)) {
            return null;
        }

        return DatetimeHelper.parseDate(value);
    }

    public OffsetDateTime parseOffsetDateTime(final String value) {
        if (value.equals(this.nullValue)) {
            return null;
        }

        return DatetimeHelper.parseOffsetDateTime(value);
    }

    public List<Double> parseGeoPoint(final String value) {
        if (value.equals(this.nullValue)) {
            return null;
        }
        final String[] tokens = value.trim().split("\\s+");
        if (tokens.length != 2) {
            throw new BadCsvEntryException("Geo coordinate '" + value + "' must contain exactly two space-separated numbers.");
        }
        try {
            final double lon = Double.parseDouble(tokens[0]);
            final double lat = Double.parseDouble(tokens[1]);
            FireflyGeoValue.validateCoordinates(lon, lat);
            return List.of(lon, lat);
        } catch (final RuntimeException e) {
            throw new BadCsvEntryException(e);
        }
    }

    public Object parseGeo(final String value, final boolean multiValue) {
        if (value.equals(this.nullValue)) {
            return null;
        }
        if (!multiValue) {
            return parseGeoPoint(value);
        }
        final String[] pointValues = value.split(";");
        final List<List<Double>> points = new ArrayList<>(pointValues.length);
        for (final String pointValue : pointValues) {
            points.add(parseGeoPoint(pointValue));
        }
        return points;
    }

    public static Object parseId(final String id) {
        try {
            return Long.parseLong(id);
        } catch (final NumberFormatException ignored) {
        }
        return id;
    }
}
