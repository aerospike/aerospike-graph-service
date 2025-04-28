package com.aerospike.firefly.runtime.tasks;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.policy.ScanPolicy;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;

import static com.aerospike.firefly.process.call.metadata.MetadataServiceUsage.MILLISECONDS_TO_HOURS;


/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyUsageStats {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyUsageStats.class);

    private FireflyUsageStatsTask task;

    // Does not need to be closed because it is a daemon thread.
    private Timer taskTimer = new Timer(true);

    public FireflyUsageStats(final AerospikeConnection connection) {
        init(connection);
    }

    private void init(final AerospikeConnection connection) {
        if (connection.shouldCreateIndexes()) {
            final List<String> setIndex = AerospikeConnection.InfoOps.createSetIndex(connection, connection.USAGE_STATS_SET);
            for (final String index : setIndex) {
                if (!"ok".equals(index)) {
                    LOG.error("Error creating set index: {}", index);
                }
            }
        }

        this.task = new FireflyUsageStatsTask(connection);

        // Schedule to run every USAGE_STATS_UPDATE_INTERVAL milliseconds.
        // Note, on initialization the task is run with extra write info,
        // so delay can be set to USAGE_STATS_UPDATE_INTERVAL.
        taskTimer.scheduleAtFixedRate(task,
                connection.USAGE_STATS_UPDATE_INTERVAL,
                connection.USAGE_STATS_UPDATE_INTERVAL);
    }


    // THIS IS A TEST ONLY FUNCTION.
    // Without this the test cannot reset the usage stats with a lower update interval.
    public void restartUsageStats(final AerospikeConnection connection) {
        taskTimer.cancel();
        taskTimer = new Timer(true);
        init(connection);
    }

    //let's help GC a bit
    public void close() {
        if (taskTimer != null) {
            taskTimer.cancel();
            taskTimer = null;
        }
        task = null;
    }

    public List<Map<String, Object>> readMetadata() {
        // shutdown started
        if (task == null) {
            return List.of();
        }
        return task.getAllUsageStats();
    }

    public double getTotalVcpuHours(final List<Map<String, Object>> usageStats, final Long epochOffsetMilliseconds) {
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

        return totalVcpuHrs;
    }

    protected class FireflyUsageStatsTask extends TimerTask {
        AerospikeConnection connection;
        final UUID uuid = UUID.randomUUID();
        final Key key;
        final Map<String, Object> map = new HashMap<>();
        boolean errorPrinted = false;

        public FireflyUsageStatsTask(final AerospikeConnection connection) {
            this.connection = connection;
            key = new Key(connection.getNamespace(), connection.USAGE_STATS_SET, uuid.toString());
            map.put("uuid", uuid.toString());
            map.put("vcpus", Runtime.getRuntime().availableProcessors());
            map.put("memory-gb", Runtime.getRuntime().maxMemory() / (1024 * 1024 * 1024));
            map.put("epoch-ms-start", Instant.now().toEpochMilli());
            map.put("epoch-ms-final", Instant.now().toEpochMilli());
            final Bin bin = new Bin(connection.USAGE_STATS_BIN, map);
            connection.checkedPut(null, key, bin);
        }

        @Override
        public void run() {
            try {
                // Each unique node will have a unique UUID that is their record key.
                map.put("epoch-ms-final", Instant.now().toEpochMilli());
                final Bin bin = new Bin(connection.USAGE_STATS_BIN, map);
                connection.writeOperate(null, key, Operation.put(bin));
                errorPrinted = false;
            } catch (final Exception ex) {
                if (!errorPrinted)
                    LOG.error("Error in FireflyUsageStats update thread:", ex);
                errorPrinted = true;
            }
        }

        public List<Map<String, Object>> getAllUsageStats() {
            final Queue<Map<String, Object>> usageStatsList = new ConcurrentLinkedQueue<>();
            try {
                this.connection.scanAll(null, connection.USAGE_STATS_SET, (key, record) -> {
                    final Map<String, Object> originalMap = (Map<String, Object>) record.getMap(connection.USAGE_STATS_BIN);
                    final Long epochDelta = (Long) originalMap.get("epoch-ms-final") - (Long) originalMap.get("epoch-ms-start");
                    if (epochDelta > 60 * 60 * 1000) {
                        originalMap.put("epoch-delta-hrs", epochDelta / (60 * 60 * 1000));
                    } else if (epochDelta > 60 * 1000) {
                        originalMap.put("epoch-delta-mins", epochDelta / (60 * 1000));
                    } else {
                        originalMap.put("epoch-delta-secs", epochDelta / 1000);
                    }
                    usageStatsList.add(originalMap);
                });
                errorPrinted = false;
            } catch (final Exception ex) {
                if (!errorPrinted) {
                    LOG.error("Error getting usage stats", ex);
                }
                errorPrinted = true;
            }
            return new ArrayList<>(usageStatsList);
        }
    }
}
