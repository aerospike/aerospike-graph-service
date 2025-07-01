package com.aerospike.firefly.bulkloader.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.Base64;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

public class PropertyValueParser {
    private final String nullValue;
    private static final List<String> DATE_PATTERNS = Arrays.asList(
            "yyyy-MM-dd",
            "yyyy-MM-dd'T'HH:mm",
            "yyyy-MM-dd'T'HH:mm:ss",
            "yyyy-MM-dd'T'HH:mm:ssX"  // Handles 'Z' as UTC
    );

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

    public Object parseBlob(final String value) {
        if (value.equals(this.nullValue)) {
            return null;
        }

        return Base64.getDecoder().decode(value);
    }

    public Date parseDate(final String value) {
        if (value.equals(this.nullValue)) {
            return null;
        }

        for (final String pattern : DATE_PATTERNS) {
            try {
                final SimpleDateFormat sdf = new SimpleDateFormat(pattern, Locale.US);
                sdf.setLenient(false);
                sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
                final Date parsed = sdf.parse(value);
                if (sdf.format(parsed).equals(value)) {
                    return parsed;
                }
            } catch (final ParseException ignored) {
                // try the next pattern
            }
        }
        throw new IllegalArgumentException("Unsupported date format: " + value);
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
