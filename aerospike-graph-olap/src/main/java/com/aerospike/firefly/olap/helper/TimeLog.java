package com.aerospike.firefly.olap.helper;

import com.aerospike.firefly.structure.FireflyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TimeLog {
    private static final Logger LOGGER = LoggerFactory.getLogger(TimeLog.class);
    private static final TimeLog instance = new TimeLog();

    Long lastLogTime = Instant.now().toEpochMilli();
    final Map<String, Long> durations = new ConcurrentHashMap<>();

    public static void reset() {
        instance.durations.clear();
        instance.lastLogTime = Instant.now().toEpochMilli();
    }

    public static void complete(final String message) {
        instance.durations.putIfAbsent(message, 0L);
        instance.durations.put(message, instance.durations.getOrDefault(message, 0L) + Instant.now().toEpochMilli() - instance.lastLogTime);
        instance.lastLogTime = Instant.now().toEpochMilli();
    }

    public static void log(final FireflyGraph graph) {
        final StringBuilder sb = new StringBuilder();
        graph.logMessage("Time log keys: " + instance.durations.keySet(), LOGGER);
        instance.durations.forEach((k, v) -> sb.append(k).append(":").append(v).append("ms, "));
        graph.logMessage("Time log: " + sb, LOGGER);
    }
}
