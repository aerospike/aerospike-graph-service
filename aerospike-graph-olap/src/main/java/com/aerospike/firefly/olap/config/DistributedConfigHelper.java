package com.aerospike.firefly.olap.config;

import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.spark.storage.StorageLevel;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class DistributedConfigHelper implements Serializable {

    private static final int MAX_CONNECTIONS_PER_NODE_OLAP_EXECUTOR = 20;
    private static final int MIN_CONNECTIONS_PER_NODE_OLAP_EXECUTOR = 20;

    private final Map<String, Object> fileConfig;
    private final Map<String, Object> olapConfig;

    private static final String OLAP_PREFIX = "aerospike.graph.olap.";
    private static final String OLAP_BATCH_JOB_SIZE = OLAP_PREFIX + "batch.job.size";
    private static final String DEBUG_DF = OLAP_PREFIX + "debug.df";
    private static final String PERSISTENCE = OLAP_PREFIX + "persist";
    private static final String SUPERNODE_STEPPING = OLAP_PREFIX + "supernode.stepping";
    private static final boolean DEBUG_DF_DEFAULT = false;
    private static final boolean SUPERNODE_STEPPING_DEFAULT = true;
    private static final String PARTITIONS = OLAP_PREFIX + "partitions";
    private static final String DISABLE_BULKING = OLAP_PREFIX + "disable.bulk.row.set";

    private static final Map<String, Object> OLAP_FIREFLY_CONFIG = Map.of(
            ConfigurationHelper.Keys.OLAP_ENABLED, true,
            ConfigurationHelper.Keys.PAGINATION_PAGE_SIZE, 5000,
            ConfigurationHelper.Keys.PAGINATION_PAGE_QUEUE_SIZE, 5,
            ConfigurationHelper.Keys.AUTO_PRE_HEAT, false,
            ConfigurationHelper.Keys.HTTP_ENABLED, false,
            ConfigurationHelper.Keys.SUMMARY_TICKER_ENABLED_FLAG, false,
            ConfigurationHelper.Keys.MAX_CONNECTIONS_PER_NODE, MAX_CONNECTIONS_PER_NODE_OLAP_EXECUTOR,
            ConfigurationHelper.Keys.MIN_CONNECTIONS_PER_NODE, MIN_CONNECTIONS_PER_NODE_OLAP_EXECUTOR
    );

    public DistributedConfigHelper(final Map<String, Object> fireflyConfig, final Map<String, Object> olapConfig) {
        this.fileConfig = fireflyConfig;
        this.olapConfig = olapConfig;
        if (this.olapConfig != null) {
            final Set<String> keys = new HashSet<>(olapConfig.keySet());
            for (final String key : keys) {
                if (key.startsWith("aerospike") && !key.startsWith(OLAP_PREFIX) && !key.startsWith(QueryParameters.OLAP_PREFIX)) {
                    this.fileConfig.put(key, olapConfig.get(key));
                    this.olapConfig.remove(key);
                }
            }
        }
    }

    public Configuration getFireflyConfig() {
        for (final Map.Entry<String, Object> entry : OLAP_FIREFLY_CONFIG.entrySet()) {
            this.fileConfig.putIfAbsent(entry.getKey(), entry.getValue());
        }
        return new MapConfiguration(this.fileConfig);
    }

    public Configuration getOlapConfig() {
        return new MapConfiguration(this.olapConfig);
    }

    public boolean isDebugDf() {
        return getOlapConfig().getBoolean(DEBUG_DF, DEBUG_DF_DEFAULT);
    }

    public boolean isSupernodeSteppingEnabled() {
        return getOlapConfig().getBoolean(SUPERNODE_STEPPING, SUPERNODE_STEPPING_DEFAULT);
    }

    public Optional<Integer> getPartitions() {
        return Optional.ofNullable(getOlapConfig().getInteger(PARTITIONS, null));
    }

    public StorageLevel getStorageLevel() {
        final String persistence = getOlapConfig().getString(PERSISTENCE, "DISK_ONLY");
        switch (persistence) {
            case "MEMORY":
            case "MEMORY_ONLY":
                return StorageLevel.MEMORY_ONLY();
            case "DISK":
            case "DISK_ONLY":
                return StorageLevel.DISK_ONLY();
            case "MEMORY_AND_DISK":
                return StorageLevel.MEMORY_AND_DISK();
            case "MEMORY_ONLY_SER":
                return StorageLevel.MEMORY_ONLY_SER();
            case "MEMORY_AND_DISK_SER":
                return StorageLevel.MEMORY_AND_DISK_SER();
            case "DISK_ONLY_2":
                return StorageLevel.DISK_ONLY_2();
            case "MEMORY_ONLY_2":
                return StorageLevel.MEMORY_ONLY_2();
            case "MEMORY_AND_DISK_2":
                return StorageLevel.MEMORY_AND_DISK_2();
            case "MEMORY_ONLY_SER_2":
                return StorageLevel.MEMORY_ONLY_SER_2();
            case "MEMORY_AND_DISK_SER_2":
                return StorageLevel.MEMORY_AND_DISK_SER_2();
            case "OFF_HEAP":
                return StorageLevel.OFF_HEAP();
            default:
                throw new IllegalArgumentException("Unsupported storage level: " + persistence +
                        ". Supported values are: MEMORY_ONLY, DISK_ONLY, MEMORY_AND_DISK, MEMORY_ONLY_SER, " +
                        "MEMORY_AND_DISK_SER, DISK_ONLY_2, MEMORY_ONLY_2, MEMORY_AND_DISK_2, MEMORY_ONLY_SER_2, " +
                        "MEMORY_AND_DISK_SER_2, OFF_HEAP.");
        }
    }

    public int getBatchJobSize() {
        return getOlapConfig().getInt(OLAP_BATCH_JOB_SIZE, 50000);
    }

    public boolean isBulkingDisabled() {
        return getOlapConfig().getBoolean(DISABLE_BULKING, false);
    }
}
