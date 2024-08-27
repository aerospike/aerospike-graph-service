package com.aerospike.firefly.process.call.metadata;

import com.aerospike.firefly.runtime.tasks.FireflyUsageStats;
import com.aerospike.firefly.structure.FireflyGraph;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetadataServiceUsage<I, R> extends MetadataServiceBase<I, R> {
    public static final Long MILLISECONDS_TO_HOURS = 1000 * 60 * 60L;
    public static final Long HOURS_TO_YEARS = 24 * 365L;
    private static final String PATTERN = "yyyy-MM-dd";

    public MetadataServiceUsage(final FireflyGraph graph) {
        super(graph);
    }

    @Override
    protected String getAdminServiceName() {
        return "usage";
    }

    @Override
    protected String usage(final Map params) {
        return String.format("Illegal arguments provided to '%s'.\n" +
                "\tExpected either no arguments provided or 'since' with a value in format '%s'.\n" +
                "\tProvided arguments: '%s'.\n" +
                "\tExample of correct usage:\n" +
                "\t\tg.call(\"%s\").next();\n" +
                "\t\t\tor\n" +
                "\t\tg.call(\"%s\").with(\"since\", \"1993-03-30\").next();",
                getName(), PATTERN, params, getName(), getName());
    }

    @Override
    protected boolean sanitize(final Map params) {
        if (params.isEmpty()) {
            return true;
        } else if (params.containsKey("since") && params.get("since") instanceof String) {
            final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(PATTERN);
            try {
                final Date date = simpleDateFormat.parse((String) params.get("since"));
                date.getTime();
                return true;
            } catch (final ParseException e) {
                return false;
            }
        }
        return false;
    }

    @Override
    protected R execute(final Map params) {
        Long epochOffsetMilliseconds = null;
        if (params.containsKey("since")) {
            final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(PATTERN);
            try {
                final Date date = simpleDateFormat.parse((String) params.get("since"));
                epochOffsetMilliseconds = date.getTime();
            } catch (final ParseException e) {
                return (R) usage(params);
            }
        }

        final List<Map<String, Object>> usageStats = FireflyUsageStats.readMetadata();
        final Map<String, Object> results = new HashMap<>();
        Double vcpuHours = FireflyUsageStats.getTotalVcpuHours(usageStats, epochOffsetMilliseconds);
        // Truncate to 2 decimal places.
        vcpuHours = Math.round(vcpuHours * 100.0) / 100.0;
        results.put("raw", usageStats);
        results.put("total-vcpu-hours", vcpuHours);
        if (epochOffsetMilliseconds != null) {
            // Get time between now and epochOffsetMilliseconds.
            final Long now = System.currentTimeMillis();
            final Long diff = now - epochOffsetMilliseconds;
            final Long diffHours = diff / MILLISECONDS_TO_HOURS;
            Double vcpuHoursPerYear = vcpuHours / diffHours;
            // Truncate to 2 decimal places.
            vcpuHoursPerYear = Math.round(vcpuHoursPerYear * 100.0) / 100.0;
            results.put("estimated-annual-total-vcpus", vcpuHoursPerYear);
        }
        return (R) results;
    }


    @Override
    public Map<String, String> describeParams() {
        return Map.of("since", "Return usage stats since a certain date in format '" + PATTERN + "'.");
    }

    @Override
    protected void auditLog(final Map params) {
        LOGGER.info("[{}] - {} - Get graph usage.", getUser(), getName());
    }
}
