package com.aerospike.firefly.runtime.tasks;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Bin;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;


/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyUsageStats {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyUsageStats.class);

    private static FireflyUsageStats instance;
    private final FireflyUsageStatsTask task;

    // Does not need to be closed because it is a daemon thread.
    private final Timer taskTimer = new Timer(true);

    private FireflyUsageStats(final AerospikeConnection connection) {
        this.task = new FireflyUsageStatsTask(connection);

        // Schedule to run every USAGE_STATS_UPDATE_INTERVAL milliseconds.
        // Note, on initialization the task is run with extra write info,
        // so delay can be set to USAGE_STATS_UPDATE_INTERVAL.
        taskTimer.scheduleAtFixedRate(task,
                connection.USAGE_STATS_UPDATE_INTERVAL,
                connection.USAGE_STATS_UPDATE_INTERVAL);
    }

    public static void startUsageStats(final AerospikeConnection connection) {
        synchronized (FireflyUsageStats.class) {
            // Don't start in warm up due to config differences.
            if (connection.WARMUP_MODE) {
                return;
            }

            // Only create once.
            if (instance == null) {
                instance = new FireflyUsageStats(connection);
            } else {
                // Update connection. Multiple open and closes can invalidate previous connection.
                instance.task.connection = connection;
            }
        }
    }

    // THIS IS A TEST ONLY FUNCTION.
    // Without this the test cannot reset the usage stats with a lower update interval.
    public static void restartUsageStats(final AerospikeConnection connection) {
        synchronized (FireflyUsageStats.class) {
            // Only create once.
            if (instance != null) {
                instance.taskTimer.cancel();
                instance = new FireflyUsageStats(connection);
            }
        }
    }

    public static List<Map<String, Object>> readMetadata() {
        synchronized (FireflyUsageStats.class) {
            if (instance == null) {
                throw new RuntimeException("Error, cannot read usage stats before starting usage stats.");
            }
            return instance.task.getAllUsageStats();
        }
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
            connection.getClient().put(null, key, bin);
        }

        @Override
        public void run() {
            try {
                // Each unique node will have a unique UUID that is their record key.
                map.put("epoch-ms-final", Instant.now().toEpochMilli());
                final Bin bin = new Bin(connection.USAGE_STATS_BIN, map);
                connection.operate(null, key, Operation.put(bin));
                errorPrinted = false;
            } catch (final Exception ex) {
                if (!errorPrinted)
                    LOG.error("Error in FireflyUsageStats update thread:", ex);
                errorPrinted = true;
            }
        }

        public List<Map<String, Object>> getAllUsageStats() {
            final List<Map<String, Object>> usageStatsList = new ArrayList<>();
            final AerospikeClient client = connection.getClient();
            client.scanAll(null, connection.getNamespace(), connection.USAGE_STATS_SET, (key, record)
                    -> usageStatsList.add((Map<String, Object>) record.getMap(connection.USAGE_STATS_BIN)));
            return usageStatsList;
        }
    }
}
