/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.olap.config;

import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.spark.storage.StorageLevel;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class DistributedConfigHelper implements Serializable {

    private static final int MAX_CONNECTIONS_PER_NODE_OLAP_EXECUTOR = 20;
    private static final int MIN_CONNECTIONS_PER_NODE_OLAP_EXECUTOR = 20;

    private final Map<String, Object> fileConfig;
    private final Map<String, Object> olapConfig;
    private final boolean isAlgorithmProgram;

    public static final String OLAP_PREFIX = "aerospike.graph.analytics.";
    private static final String OLAP_BATCH_JOB_SIZE = OLAP_PREFIX + "batch.job.size";
    private static final String DEBUG_DF = OLAP_PREFIX + "debug.df";
    private static final String FORCE_GC = OLAP_PREFIX + "debug.gc";
    private static final String PERSISTENCE = OLAP_PREFIX + "persist";
    private static final String SUPERNODE_STEPPING = OLAP_PREFIX + "supernode.stepping";
    private static final String TEMP_WRITE_DIRECTORY = OLAP_PREFIX + "temp.write.directory";
    private static final String TEMP_WRITE_DISABLED = OLAP_PREFIX + "temp.write.disabled";
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
    final String randomTempDir;

    public DistributedConfigHelper(final Map<String, Object> fireflyConfig, final Map<String, Object> olapConfig, final boolean isAlgorithmProgram) {
        this.fileConfig = fireflyConfig;
        this.olapConfig = olapConfig;
        this.isAlgorithmProgram = isAlgorithmProgram;
        randomTempDir = RandomStringUtils.randomAlphanumeric(8);
        if (this.olapConfig != null) {
            final Set<String> keys = new HashSet<>(olapConfig.keySet());
            for (final String key : keys) {
                if (key.startsWith("aerospike") && !key.startsWith(OLAP_PREFIX)) {
                    this.fileConfig.put(key, olapConfig.get(key));
                    this.olapConfig.remove(key);
                }
            }
        }

        validateConfig();
    }

    private void validateConfig() {
        if (isAlgorithmProgram) {
            final boolean requiresTempWriteDirectory = !getOlapConfig().getBoolean(TEMP_WRITE_DISABLED, false);

            if (requiresTempWriteDirectory && getOlapConfig().getString(TEMP_WRITE_DIRECTORY) == null) {
                throw new IllegalStateException(
                        "Temporary write directory must be specified for algorithm programs."
                                + " Please set the '" + TEMP_WRITE_DIRECTORY + "' configuration property."
                                + " Or set the '" + TEMP_WRITE_DISABLED + "' to `true`.");
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

    public boolean isForceGC() {
        return getOlapConfig().getBoolean(FORCE_GC, false);
    }

    public boolean isSupernodeSteppingEnabled() {
        return getOlapConfig().getBoolean(SUPERNODE_STEPPING, SUPERNODE_STEPPING_DEFAULT);
    }

    public Optional<Integer> getPartitions() {
        return Optional.ofNullable(getOlapConfig().getInteger(PARTITIONS, null));
    }

    public StorageLevel getStorageLevel() {
        final String persistence = getOlapConfig().getString(PERSISTENCE, isAlgorithmProgram ? "DISK_ONLY" : "MEMORY_AND_DISK");

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
        return getOlapConfig().getInt(OLAP_BATCH_JOB_SIZE, 5000);
    }

    public boolean isBulkingDisabled() {
        return getOlapConfig().getBoolean(DISABLE_BULKING, true);
    }

    public String getTempWriteDirectory() {
        if (getOlapConfig().getBoolean(TEMP_WRITE_DISABLED, false)) {
            return "";
        }

        final String tempDir = getOlapConfig().getString(TEMP_WRITE_DIRECTORY, "");
        if (!tempDir.isEmpty()) {
            final String separator = "/";
            return tempDir + separator + "AGA" + separator + randomTempDir;
        } else {
            return "";
        }
    }
}
