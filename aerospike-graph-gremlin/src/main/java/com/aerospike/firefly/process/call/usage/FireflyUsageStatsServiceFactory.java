package com.aerospike.firefly.process.call.usage;

import com.aerospike.firefly.runtime.tasks.FireflyUsageStats;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.service.Service;
import org.apache.tinkerpop.gremlin.structure.util.CloseableIterator;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.structure.service.Service.Type.Start;

public class FireflyUsageStatsServiceFactory<I, R> implements Service.ServiceFactory<I, R>, Service<I, R> {
    public static final Long MILLISECONDS_TO_HOURS = 1000 * 60 * 60L;
    public static final Long HOURS_TO_YEARS = 24 * 365L;

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
        final List<Map<String, Object>> usageStats = FireflyUsageStats.readMetadata();
        final Map<String, Object> results = new HashMap<>();
        results.put("raw", usageStats);
        double totalVcpuHrs = 0.0;
        for (Map<String, Object> usageStat : usageStats) {
            final Long start = (Long) usageStat.get("epoch-ms-start");
            final Long end = (Long) usageStat.get("epoch-ms-final");
            final Long vcpus = (Long) usageStat.get("vcpus");

            // Total vcpu hours is sum of number of hours * number of vcpus.
            totalVcpuHrs += ((double) (end - start) / (double) MILLISECONDS_TO_HOURS) * vcpus;
        }
        results.put("total-vcpu-hrs", totalVcpuHrs);
        results.put("total-vcpu-yrs", totalVcpuHrs / HOURS_TO_YEARS);
        return CloseableIterator.of(IteratorUtils.of((R) results));
    }


    @Override
    public Map<String, String> describeParams() {
        return Map.of();
    }

    @Override
    public void close() {
        ServiceFactory.super.close();
        Service.super.close();
    }
}
