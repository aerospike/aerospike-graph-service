package com.aerospike.firefly.process.call.usage;

import com.aerospike.firefly.runtime.tasks.FireflyUsageStats;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.structure.service.Service.Type.Start;

public class FireflyUsageStatsServiceFactory<I, R> implements Service.ServiceFactory<I, R>, Service<I, R> {
    public static final Long MILLISECONDS_TO_HOURS = 1000 * 60 * 60L;
    public static final Long HOURS_TO_YEARS = 24 * 365L;
    private static final String PATTERN = "yyyy-MM-dd";

    @Override
    public String getName() {
        return "usage-stats";
    }

    @Override
    public Set<Type> getSupportedTypes() {
        return Set.of(Type.Start);
    }

    @Override
    public Service<I, R> createService(final boolean isStart, final Map params) {
        if (!isStart) {
            throw new UnsupportedOperationException(Service.Exceptions.cannotUseMidTraversal);
        }
        return this;
    }

    @Override
    public Type getType() {
        return Start;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Service.super.getRequirements();
    }

    @Override
    public CloseableIterator<R> execute(final ServiceCallContext ctx, final Map params) {
        Long epochOffsetMilliseconds = null;
        if (params.containsKey("since")) {
            if (params.get("since") == null) {
                throw new IllegalArgumentException("Failed to parse provided date '" + params.get("since") + "'. " +
                        "Expected date provided to be in format '" + PATTERN + "'. Provided date was null.");
            } else if (!(params.get("since") instanceof String)) {
                throw new IllegalArgumentException("Failed to parse provided date '" + params.get("since") + "'. " +
                        "Expected date provided to be in format '" + PATTERN + "'. Provided date was not a String.");
            }
            final SimpleDateFormat simpleDateFormat = new SimpleDateFormat(PATTERN);
            try {
                final Date date = simpleDateFormat.parse((String) params.get("since"));
                epochOffsetMilliseconds = date.getTime();
            } catch (final ParseException e) {
                throw new IllegalArgumentException("Failed to parse provided date '" + params.get("since") + "'. " +
                        "Expected date provided to be in format '" + PATTERN + "'. " + e.getMessage());
            }
        } else if (!params.keySet().isEmpty()) {
            throw new IllegalArgumentException("Invalid parameters: " + params.keySet());
        }

        final List<Map<String, Object>> usageStats = FireflyUsageStats.readMetadata();
        final Map<String, Object> results = new HashMap<>();
        results.put("raw", usageStats);
        double totalVcpuHrs = 0.0;
        for (Map<String, Object> usageStat : usageStats) {
            Long start = (Long) usageStat.get("epoch-ms-start");
            Long end = (Long) usageStat.get("epoch-ms-final");
            final Long vcpus = (Long) usageStat.get("vcpus");

            // Only use if offset is provided.
            if (epochOffsetMilliseconds != null) {
                if (start < epochOffsetMilliseconds) {
                    start = epochOffsetMilliseconds;
                }
                if (end < epochOffsetMilliseconds) {
                    end = epochOffsetMilliseconds;
                }
            }

            // Total vcpu hours is sum of number of hours * number of vcpus.
            totalVcpuHrs += ((double) (end - start) / (double) MILLISECONDS_TO_HOURS) * vcpus;
        }

        // total-vcpu is a measure of vcpu-years.
        results.put("total-vcpu", totalVcpuHrs / HOURS_TO_YEARS);
        return CloseableIterator.of(IteratorUtils.of((R) results));
    }


    @Override
    public Map<String, String> describeParams() {
        return Map.of("since", "Return usage stats since a certain date in format '" + PATTERN + "'.");
    }

    @Override
    public void close() {
        ServiceFactory.super.close();
        Service.super.close();
    }
}
