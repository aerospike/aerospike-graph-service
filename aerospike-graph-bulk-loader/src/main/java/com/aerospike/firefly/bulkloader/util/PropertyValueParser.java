package com.aerospike.firefly.bulkloader.util;

import java.time.OffsetDateTime;
import java.util.Date;

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

    public static Object parseId(final String id) {
        try {
            return Long.parseLong(id);
        } catch (final NumberFormatException ignored) {
        }
        return id;
    }
}
