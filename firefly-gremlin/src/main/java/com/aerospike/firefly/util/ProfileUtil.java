package com.aerospike.firefly.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public final class ProfileUtil {
    static Map<String, Map<String, AtomicLong>> metrics = new HashMap<>();

    private ProfileUtil() {
    }

    public static void increment(Class<?> clazz, String functionName) {
        Map<String, AtomicLong> clazzMetrics = metrics.getOrDefault(clazz.getName(), new HashMap<>());
        AtomicLong functionMetric = clazzMetrics.getOrDefault(functionName, new AtomicLong());
        functionMetric.addAndGet(1);
        clazzMetrics.put(functionName, functionMetric);
        metrics.put(clazz.getName(), clazzMetrics);
    }

    public static void reset() {
        metrics.clear();
    }

    public static String report() {
        final List<String> results = new ArrayList<>();
        metrics.forEach((clazz, clazzMetrics) -> {
            clazzMetrics.forEach((fn, callCount) -> {
                results.add(String.format("%s.%s: %d calls", clazz, fn, callCount.get()));
            });
        });
        return results.stream().reduce("", (a, b) -> a + "\n" + b);
    }

}
