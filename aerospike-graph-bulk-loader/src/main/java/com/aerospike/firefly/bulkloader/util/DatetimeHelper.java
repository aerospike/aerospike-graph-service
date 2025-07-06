package com.aerospike.firefly.bulkloader.util;

import java.time.*;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Date;

import static java.time.ZoneOffset.UTC;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE;
import static java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME;

/**
 * A datetime utility class to align with TinkerPop datetime parsing.
 * Support both (for backward compatibility):
 * 1. java.util.Date (the official date format in TinkerPop 3.7).
 * 2. OffsetDateTime (the official date format starting with TinkerPop 4.0).
 */
public class DatetimeHelper {
    /**
     * Seems like the "noOffsetText" needs to only be set to "Z" once - doing it twice duplicates the "Z" on
     * {@code format()} calls.
     */
    private static final DateTimeFormatter datetimeFormatter = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .append(ISO_LOCAL_DATE_TIME)
            .optionalStart()
            .appendOffset("+HHMMss", "Z")
            .optionalEnd()
            .optionalStart()
            .appendOffset("+HH:MM:ss", "")
            .optionalEnd()
            .toFormatter();

    private static final DateTimeFormatter yearMonthFormatter = new DateTimeFormatterBuilder()
            .parseCaseInsensitive()
            .appendValue(ChronoField.YEAR)
            .appendLiteral('-')
            .appendValue(ChronoField.MONTH_OF_YEAR).toFormatter().withResolverStyle(ResolverStyle.LENIENT);

    private static final DateTimeFormatter formatter = new DateTimeFormatterBuilder()
            .appendOptional(datetimeFormatter)
            .appendOptional(ISO_LOCAL_DATE)
            .appendOptional(yearMonthFormatter)
            .toFormatter();

    private DatetimeHelper() {
    }

    /**
     * Formats an {@code Instant} to a form of {@code 2018-03-22T00:35:44Z} at UTC.
     */
    public static String format(final Instant d) {
        return datetimeFormatter.format(d.atZone(UTC));
    }

    /**
     * Parses a {@code String} representing a date and/or time to a {@code Date} object with a default time zone offset
     * of UTC (+00:00). It can parse dates in any of the following formats.
     *
     * <ul>
     *     <li>2018-03-22</li>
     *     <li>2018-03-22T00:35:44</li>
     *     <li>2018-03-22T00:35:44Z</li>
     *     <li>2018-03-22T00:35:44.741</li>
     *     <li>2018-03-22T00:35:44.741Z</li>
     *     <li>2018-03-22T00:35:44.741+1600</li>
     *     <li>2018-03-22T00:35:44.741+16:00</li>
     *     <li>2018-03-22T00:35:44.741+160000</li>
     *     <li>2018-03-22T00:35:44.741+16:00:00</li>
     * </ul>>
     */
    public static Date parseDate(final String d) {
        final TemporalAccessor t = formatter.parse(d);

        if (!t.isSupported(ChronoField.HOUR_OF_DAY)) {
            // no hours field so it must be a Date or a YearMonth
            if (!t.isSupported(ChronoField.DAY_OF_MONTH)) {
                // must be a YearMonth coz no day
                return Date.from(YearMonth.from(t).atDay(1).atStartOfDay(UTC).toInstant());
            } else {
                // must be a Date as the day is present
                return Date.from(Instant.ofEpochSecond(LocalDate.from(t).atStartOfDay().toEpochSecond(UTC)));
            }
        } else if (!t.isSupported(ChronoField.MONTH_OF_YEAR)) {
            // no month field so must be a Time
            final Instant timeOnEpochDay = LocalDate.ofEpochDay(0)
                    .atTime(LocalTime.from(t))
                    .atZone(UTC)
                    .toInstant();
            return Date.from(timeOnEpochDay);
        } else if (t.isSupported(ChronoField.OFFSET_SECONDS)) {
            // has all datetime components including an offset
            return Date.from(ZonedDateTime.from(t).toInstant());
        } else {
            // has all datetime components but no offset so throw in some UTC
            return Date.from(ZonedDateTime.of(LocalDateTime.from(t), UTC).toInstant());
        }
    }

    /**
     * Parses a {@code String} representing a date and/or time to a {@code Date} object with a default time zone offset
     * of UTC (+00:00). It can parse dates in any of the following formats.
     *
     * <ul>
     *     <li>2018-03-22</li>
     *     <li>2018-03-22T00:35:44</li>
     *     <li>2018-03-22T00:35:44Z</li>
     *     <li>2018-03-22T00:35:44.741</li>
     *     <li>2018-03-22T00:35:44.741Z</li>
     *     <li>2018-03-22T00:35:44.741+1600</li>
     *     <li>2018-03-22T00:35:44.741+16:00</li>
     *     <li>2018-03-22T00:35:44.741+160000</li>
     *     <li>2018-03-22T00:35:44.741+16:00:00</li>
     * </ul>>
     */
    public static OffsetDateTime parseOffsetDateTime(final String d) {
        final TemporalAccessor t = formatter.parse(d);

        if (!t.isSupported(ChronoField.HOUR_OF_DAY)) {
            // no hours field so it must be a Date or a YearMonth
            if (!t.isSupported(ChronoField.DAY_OF_MONTH)) {
                // must be a YearMonth coz no day
                return OffsetDateTime.of(LocalDate.of(Year.from(t).getValue(), Month.from(t), 1), LocalTime.MIDNIGHT, UTC);
            } else {
                // must be a Date as the day is present
                return OffsetDateTime.of(LocalDate.from(t), LocalTime.MIDNIGHT, UTC);
            }
        } else if (!t.isSupported(ChronoField.MONTH_OF_YEAR)) {
            // no month field so must be a Time
            return OffsetDateTime.of(LocalDate.ofEpochDay(0), LocalTime.from(t), UTC);
        } else {
            // can directly obtain from TemporalAccessor
            return OffsetDateTime.from(t);
        }
    }
}
