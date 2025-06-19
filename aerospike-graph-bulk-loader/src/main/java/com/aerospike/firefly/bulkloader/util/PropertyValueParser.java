package com.aerospike.firefly.bulkloader.util;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;

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

        return Boolean.parseBoolean(value);
    }

    public String parseString(final String value) {
        if (value.equals(this.nullValue)) {
            return null;
        }

        return value;
    }

    public LocalDate parseLocalDate(final String value) {
        if (value.equals(this.nullValue)) {
            return null;
        }

        return LocalDate.parse(value);
    }

    public LocalDateTime parseLocalDateTime(final String value) {
        if (value.equals(this.nullValue)) {
            return null;
        }

        return LocalDateTime.parse(value);
    }

    public OffsetDateTime parseOffsetDateTime(final String value) {
        if (value.equals(this.nullValue)) {
            return null;
        }

        return OffsetDateTime.parse(value);
    }

    public static Object parseId(final String id) {
        try {
            return Long.parseLong(id);
        } catch (final NumberFormatException ignored) {
        }
        return id;
    }
}
