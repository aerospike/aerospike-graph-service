package com.aerospike.firefly.util;

import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aerospike.firefly.io.aerospike.AerospikeConnection.getDefaultThreadPoolSize;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public final class ConfigurationHelper {
    private static final Logger logger = LoggerFactory.getLogger(AerospikeConnection.class);

    private ConfigurationHelper() {
    }

    private static List PREFIX_MASK = new ArrayList() {{
        add(Keys.GRAPH_ID);
        add(Keys.ON_RECORD_ID_LIMIT);
    }};

    public static class Keys {

        // External Configs
        public static final String AEROSPIKE_HOST = "aerospike.client.host";
        public static final String AEROSPIKE_PORT = "aerospike.client.port";
        public static final String AEROSPIKE_TIMEOUT = "aerospike.client.timeout";
        public static final String AEROSPIKE_USER = "aerospike.client.user";
        public static final String AEROSPIKE_PASSWORD = "aerospike.client.password";
        public static final String AEROSPIKE_NAMESPACE = "aerospike.client.namespace";
        public static final String SCAN_MAX_WAIT = "aerospike.client.scan.max.wait";
        public static final String AEROSPIKE_BATCH_READ_SIZE = "aerospike.client.batch.read.size";
        public static final String AEROSPIKE_WRITE_MAX_RETRY = "aerospike.client.write.max.retry";
        public static final String TLS = "aerospike.client.tls";
        public static final String TLS_NAMES = "aerospike.client.tls.name";
        public static final String LOG_LEVEL = "aerospike.graph.log.level";
        public static final String FIREFLY_DATA_MODEL = "aerospike.graph.data.model";
        public static final String ADJACENCY_INDEX_ENABLED_FLAG = "aerospike.graph.index.adjacency.enabled";
        public static final String V_LABEL_INDEX_ENABLED_FLAG = "aerospike.graph.index.vertex.label.enabled";
        public static final String E_LABEL_INDEX_ENABLED_FLAG = "aerospike.graph.index.edge.label.enabled";
        public static final String SUMMARY_ENABLED_FLAG = "aerospike.graph.summary.enabled";
        public static final String SUMMARY_TICKER_ENABLED_FLAG = "aerospike.graph.summary.ticker.enabled";
        public static final String PHAT_EDGE_SIZE = "aerospike.graph.phat.edge.size";
        public static final String MOVEMENT_BARRIER_SIZE = "aerospike.graph.movement.barrier.size";
        public static final String VERTEX_PROPERTY_INDEXES = "aerospike.graph.index.vertex.properties";
        public static final String EDGE_PROPERTY_INDEXES = "aerospike.graph.index.edge.properties";
        public static final String GRAPH_ID = "aerospike.graph.id";
        public static final String PROMETHEUS_PORT = "aerospike.graph.prometheus.port";
        public static final String PROMETHEUS_PATH = "aerospike.graph.prometheus.path";
        public static final String PLUGIN = "aerospike.graph.plugin";
        public static final String TTL_ENABLED_FLAG = "aerospike.graph.ttl.enabled";
        public static final String TTL_PURGE_INTERVAL_SECONDS = "aerospike.graph.ttl.purge.interval";
        public static final String TTL_UPDATE_ANYTIME_FLAG = "aerospike.graph.ttl.update.anytime.enabled";

        // Semi internal semi external configs
        public static final String FIREFLY_READ_THROUGH_CACHE_WEIGHT = "aerospike.graph.cache.weight";
        public static final String INDEX_METADATA_UPDATE_FREQUENCY = "aerospike.graph.metadata.index.update.frequency";
        public static final String CARDINALITY_METADATA_UPDATE_FREQUENCY = "aerospike.graph.metadata.cardinality.update.frequency";
        public static final String ENABLE_FAST_COUNT_STRATEGY = "aerospike.graph.strategy.fast.count.enabled";
        public static final String ENABLE_READ_THROUGH_CACHE = "aerospike.graph.strategy.cache.read.through.enabled";
        public static final String ENABLE_PREFETCH_STRATEGY = "aerospike.graph.strategy.prefetch.enabled";
        public static final String ENABLE_FIREFLY_DROP_STRATEGY = "aerospike.graph.strategy.drop.enabled";
        public static final String ENABLE_COMPOSITE_ID_STRATEGY = "aerospike.graph.strategy.composite.id.enabled";
        public static final String ENABLE_EMBEDDED_COMPOSITE_ID_STRATEGY = "aerospike.graph.strategy.composite.id.embedded.enabled";
        public static final String ENABLE_COMPOSITE_ID_SAMPLING_STRATEGY = "aerospike.graph.strategy.composite.id.sampling.enabled";
        public static final String ENABLE_COMPOSITE_ID_LIMIT_STRATEGY = "aerospike.graph.strategy.composite.id.limit.enabled";
        public static final String ENABLE_BATCH_EDGE_READ_STRATEGY = "aerospike.graph.strategy.batch.edge.read.enabled";
        public static final String ENABLE_EMBEDDED_BATCH_EDGE_READ_STRATEGY = "aerospike.graph.strategy.batch.edge.read.embedded.enabled";
        public static final String ENABLE_BATCH_EDGE_READ_SAMPLING_STRATEGY = "aerospike.graph.strategy.batch.edge.read.sampling.enabled";
        public static final String ENABLE_BATCH_EDGE_READ_LIMIT_STRATEGY = "aerospike.graph.strategy.batch.edge.read.limit.enabled";
        public static final String GLOBAL_EDGE_CACHE_ENABLED = "aerospike.graph.global.edge.cache.enabled";
        public static final String VERTEX_ID_BUFFER_SIZE = "aerospike.graph.vertex.id.buffer.size";
        public static final String EDGE_ID_BUFFER_SIZE = "aerospike.graph.edge.id.buffer.size";
        public static final String PROPERTY_ID_BUFFER_SIZE = "aerospike.graph.property.id.buffer.size";
        public static final String STORAGE_DEBUGGER_FLAG = "storage.debug";

        // TODO: Once we are 100% sure these are stable, we can remove the enable flags.
        public static final String ENABLE_EMBEDDED_GRAPH_COUNT_STRATEGY = "aerospike.graph.strategy.fast.count.embedded.enabled";
        public static final String ENABLE_EMBEDDED_VERTEX_EDGE_LOCAL_COUNT_STRATEGY = "aerospike.graph.strategy.local.fast.count.embedded.enabled";
        public static final String ENABLE_BATCHED_REPEAT_STEP_STRATEGY = "aerospike.graph.strategy.batched.repeat.step.enabled";

        // Internal-only configurations
        public static final String AUTO_PRE_HEAT = "AUTO_PRE_HEAT";
        public static final String WARMUP_MODE = "WARMUP_MODE";
        public static final String FAULT_TEST = "FAULT_TEST";
        public static final String ENABLE_CUSTOM_PROFILE = "ENABLE_CUSTOM_PROFILE";
        public static final String ASCLIENT_LOG_ENABLED = "ASCLIENT_LOG_ENABLED";
        public static final String ASYNC_SUBGRAPH_CACHE = "ASYNC_SUBGRAPH_CACHE";
        public static final String OPTIMIZED_TWO_HOP_STEPS = "OPTIMIZED_TWO_HOP_STEPS";
        public static final String OPTIMIZED_HOP_CONSTRAINT_STEPS = "OPTIMIZED_HOP_CONSTRAINT_STEPS";
        public static final String ON_RECORD_ID_LIMIT = "ON_RECORD_ID_LIMIT";
        public static final String DEBUG_MODE_FLAG = "DEBUG_MODE_FLAG";
        public static final String BULK_LOADER_FLAG = "BULK_LOADER_FLAG";
        public static final String USAGE_STATS_UPDATE_INTERVAL = "USAGE_STATS_UPDATE_INTERVAL";

        public static final String CLIENT_FAILURE_TEST = "aerospike.graph.failure.client.enabled";
        public static final String CLIENT_FAILURE_RATE = "aerospike.graph.failure.client.rate";
        public static final String MAX_ERROR_RATE = "aerospike.client.maxErrorRate";
        public static final String MAX_CONNECTIONS_PER_NODE = "aerospike.client.maxConnectionsPerNode";
        public static final String MIN_CONNECTIONS_PER_NODE = "aerospike.client.minConnectionsPerNode";
        public static final String CONNECT_TIMEOUT = "aerospike.client.connectTimeout";
        public static final String TIMEOUT_DELAY = "aerospike.client.timeoutDelay";
        public static final String PROMETHEUS_RENAME = "aerospike.graph.prometheus.rename.enabled";

        public static class Pair {
            public final int numeric;
            public final String english;

            private Pair(final byte key, final String value) {
                this.numeric = key;
                this.english = value;
            }

            public static Pair of(final byte numeric, final String english) {
                return new Pair(numeric, english);
            }
        }

        public enum Bins {
            GRAPH_VARIABLES_BIN(Pair.of((byte) 1, "GRAPH_VARS")),
            VERTEX_PROPERTY_NAME_TO_VALUE_BIN(Pair.of((byte) 2, "VP_NAME_VAL")),
            VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN(Pair.of((byte) 3, "VP_HINT")),
            RELATIONAL_VERTEX_TYPE_HINT_BIN(Pair.of((byte) 4, "REL_VP_HINT")),
            EDGE_CACHE_DISABLED_BIN(Pair.of((byte) 6, "ECACHE_OFF")),
            IN_EDGES_BIN(Pair.of((byte) 7, "IN_EDGES")),
            OUT_EDGES_BIN(Pair.of((byte) 8, "OUT_EDGES")),
            PROPERTIES_BIN(Pair.of((byte) 9, "PROPERTIES")),
            TYPE_HINTS_BIN(Pair.of((byte) 10, "TYPE_HINTS")),
            COUNTER_BIN(Pair.of((byte) 11, "COUNTER")),
            ID_TYPE_BIN(Pair.of((byte) 12, "ID_TYPE")),
            USER_KEY_BIN(Pair.of((byte) 13, "USER_KEY")),
            LABEL_BIN(Pair.of((byte) 14, "LABEL")),
            IN_EDGE_COUNTER_BIN(Pair.of((byte) 15, "IN_E_C")),
            OUT_EDGE_COUNTER_BIN(Pair.of((byte) 16, "OUT_E_C")),
            VERTEX_PROPERTY_NAME_TO_ID_BIN(Pair.of((byte) 17, "VP_NAME_ID")),
            TTL_BIN(Pair.of((byte) 18, "TTL")),
            USAGE_STATS_BIN(Pair.of((byte) 19, "USAGE_STATS"));

            private final Pair value;

            Bins(final Pair b) {
                this.value = b;
            }

            public Pair getValue() {
                return value;
            }

            public static Set<String> keys() {
                return Arrays.stream(Bins.values()).map(Bins::name).collect(Collectors.toSet());
            }
        }

        public enum InternalConfigs {
            GRAPH_VARIABLES_REC_KEY(Pair.of((byte) 0, "GRAPH_VARS_REC")),
            V_LABEL_INDEX_NAME(Pair.of((byte) 4, "V_LABEL_IDX")),
            E_LABEL_INDEX_NAME(Pair.of((byte) 5, "E_LABEL_IDX")),
            E_IN_INDEX_NAME(Pair.of((byte) 6, "E_IN_IDX")),
            E_OUT_INDEX_NAME(Pair.of((byte) 7, "E_OUT_IDX")),
            TTL_EDGE_INDEX_NAME(Pair.of((byte) 8, "TTL_V_IDX")),
            TTL_VERTEX_INDEX_NAME(Pair.of((byte) 9, "TTL_E_IDX")),
            SUPERNODES_IN(Pair.of((byte) 11, "SUPERNODE_IN")),
            SUPERNODES_OUT(Pair.of((byte) 12, "SUPERNODE_OUT")),
            INDEX_METADATA_SET(Pair.of((byte) 13, "INDEX_METADATA"));

            private final Pair value;

            InternalConfigs(Pair value) {
                this.value = value;
            }

            public Pair getValue() {
                return value;
            }

            public static Set<String> keys() {
                return Arrays.stream(InternalConfigs.values()).map(InternalConfigs::name).collect(Collectors.toSet());
            }

        }

        public enum Sets {
            GRAPH_VARIABLES_SET(Pair.of((byte) 0, "GRAPH_VARS")),
            EDGE_AERO_SET(Pair.of((byte) 1, "EDGES")),
            VERTEX_AERO_SET(Pair.of((byte) 2, "VERTICES")),
            IN_VP_SET(Pair.of((byte) 3, "IN_VP")),
            OUT_VP_SET(Pair.of((byte) 4, "OUT_VP")),
            ID_MANAGER_SET(Pair.of((byte) 9, "ID_MANAGER")),
            SUMMARY_SET(Pair.of((byte) 10, "SUMMARY")),
            TEST_SET(Pair.of((byte) 11, "TEST")),
            GRAPH_METADATA_SET(Pair.of((byte) 12, "METADATA")),
            USAGE_STATS_SET(Pair.of((byte) 13, "USAGE_STATS_SET")),
            USER_SUPPLIED_ID_CACHE_SET(Pair.of((byte) 30, "ID_CACHE"));

            private final Pair value;

            public static Set<String> keys() {
                return Arrays.stream(Sets.values()).map(Sets::name).collect(Collectors.toSet());
            }

            public Pair getValue() {
                return value;
            }

            Sets(final Pair names) {
                this.value = names;
            }
        }
    }

    public static final Set<String> IMMUTABLE_CONFIG_KEYS = Set.of(
            Keys.PHAT_EDGE_SIZE, // Calculating the PK wouldn't work
            Keys.SUMMARY_ENABLED_FLAG, // Inaccurate and therefore useless if toggled
            Keys.FIREFLY_DATA_MODEL,
            Keys.DEBUG_MODE_FLAG
    );

    private static final Map<Object, String> defaultValues = new HashMap<>() {{
        put(Keys.AEROSPIKE_HOST, "localhost");
        put(Keys.AEROSPIKE_NAMESPACE, "test");
        put(Keys.AEROSPIKE_USER, "");
        put(Keys.AEROSPIKE_PASSWORD, "");
        put(Keys.GRAPH_ID, "0");
        put(Keys.ON_RECORD_ID_LIMIT, "8000");
        put(Keys.STORAGE_DEBUGGER_FLAG, "false");
        put(Keys.FIREFLY_DATA_MODEL, "packed");
        put(Keys.V_LABEL_INDEX_ENABLED_FLAG, "false");
        put(Keys.E_LABEL_INDEX_ENABLED_FLAG, "false");
        put(Keys.SCAN_MAX_WAIT, "2000");
        put(Keys.AEROSPIKE_WRITE_MAX_RETRY, "100");
        put(Keys.ENABLE_FAST_COUNT_STRATEGY, "true");
        put(Keys.ENABLE_READ_THROUGH_CACHE, "true");
        put(Keys.ENABLE_PREFETCH_STRATEGY, "true");
        put(Keys.ENABLE_FIREFLY_DROP_STRATEGY, "true");
        put(Keys.ENABLE_COMPOSITE_ID_STRATEGY, "true");
        put(Keys.ENABLE_EMBEDDED_COMPOSITE_ID_STRATEGY, "true");
        put(Keys.ENABLE_COMPOSITE_ID_SAMPLING_STRATEGY, "true");
        put(Keys.ENABLE_COMPOSITE_ID_LIMIT_STRATEGY, "true");
        put(Keys.ENABLE_BATCH_EDGE_READ_STRATEGY, "true");
        put(Keys.ENABLE_BATCH_EDGE_READ_SAMPLING_STRATEGY, "true");
        put(Keys.ENABLE_BATCH_EDGE_READ_LIMIT_STRATEGY, "true");
        put(Keys.ENABLE_EMBEDDED_BATCH_EDGE_READ_STRATEGY, "true");
        put(Keys.ENABLE_EMBEDDED_GRAPH_COUNT_STRATEGY, "true");
        put(Keys.ENABLE_EMBEDDED_VERTEX_EDGE_LOCAL_COUNT_STRATEGY, "true");
        put(Keys.ENABLE_BATCHED_REPEAT_STEP_STRATEGY, "true");
        put(Keys.ASYNC_SUBGRAPH_CACHE, "false");
        put(Keys.AEROSPIKE_PORT, "3000");
        put(Keys.AEROSPIKE_TIMEOUT, "2000");
        put(Keys.VERTEX_ID_BUFFER_SIZE, "1000");
        put(Keys.EDGE_ID_BUFFER_SIZE, "10000");
        put(Keys.PROPERTY_ID_BUFFER_SIZE, "10000");
        put(Keys.CARDINALITY_METADATA_UPDATE_FREQUENCY, "3600000"); // 1 hour default
        put(Keys.INDEX_METADATA_UPDATE_FREQUENCY, "30000"); // 30 second default
        put(Keys.GLOBAL_EDGE_CACHE_ENABLED, "true");
        put(Keys.ADJACENCY_INDEX_ENABLED_FLAG, "true");
        put(Keys.PROMETHEUS_PORT, "9090");
        put(Keys.PROMETHEUS_PATH, "/metrics");
        put(Keys.OPTIMIZED_TWO_HOP_STEPS, "");
        put(Keys.OPTIMIZED_HOP_CONSTRAINT_STEPS, "");
        put(Keys.AEROSPIKE_BATCH_READ_SIZE, "5000");
        put(Keys.FIREFLY_READ_THROUGH_CACHE_WEIGHT, "1000000");
        put(Keys.VERTEX_PROPERTY_INDEXES, "");
        put(Keys.EDGE_PROPERTY_INDEXES, "");
        put(Keys.PHAT_EDGE_SIZE, "10");
        put(Keys.MOVEMENT_BARRIER_SIZE, "1000");
        put(Keys.LOG_LEVEL, "INFO");
        put(Keys.TLS, "false");
        put(Keys.AUTO_PRE_HEAT, "false");
        put(Keys.WARMUP_MODE, "false");
        put(Keys.ENABLE_CUSTOM_PROFILE, "true");
        put(Keys.FAULT_TEST, "false");
        put(Keys.CLIENT_FAILURE_TEST, "false");
        put(Keys.CLIENT_FAILURE_RATE, "0");
        put(Keys.ASCLIENT_LOG_ENABLED, "false");
        put(Keys.SUMMARY_TICKER_ENABLED_FLAG, "true");
        put(Keys.SUMMARY_ENABLED_FLAG, "true");
        put(Keys.BULK_LOADER_FLAG, "false");
        put(Keys.MAX_ERROR_RATE, "100");
        put(Keys.MAX_CONNECTIONS_PER_NODE, String.valueOf(getDefaultThreadPoolSize(FireflyGraph.getGremlinServerSettings()) * 2));
        put(Keys.MIN_CONNECTIONS_PER_NODE, String.valueOf(getDefaultThreadPoolSize(FireflyGraph.getGremlinServerSettings())));
        put(Keys.CONNECT_TIMEOUT, "0");
        put(Keys.TIMEOUT_DELAY, "0");
        put(Keys.PROMETHEUS_RENAME, "true");
        put(Keys.DEBUG_MODE_FLAG, "false");
        put(Keys.TTL_ENABLED_FLAG, "false");
        put(Keys.TTL_PURGE_INTERVAL_SECONDS, "300"); // 5 minute default
        put(Keys.TTL_UPDATE_ANYTIME_FLAG, "false");
        put(Keys.USAGE_STATS_UPDATE_INTERVAL, "3600000"); // 1 hour default
    }};


    public static List<String> getOrDefaultList(final String key, final Configuration config) {
        // Adds a space if it is empty. Remove the space.
        final List<String> values = Arrays.stream(getOrDefaultString(key, config).split(",")).map(String::trim).collect(Collectors.toList());
        values.remove("");
        return values;
    }

    public static Configuration loadFromFile(final Path path) {
        try {
            Properties props = new Properties();
            props.load(Files.newBufferedReader(path));
            HashMap<String, Object> configData = new HashMap<>();
            props.keySet().forEach(it -> {
                final String key = it.toString().toLowerCase();
                final Object value = props.get(it.toString());
                logger.debug("config[{}:{}]", key, value);
                configData.put(key, value);
            });
            return new MapConfiguration(configData);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Configuration loadFromFile(final String path) {
        return loadFromFile(Paths.get(path));
    }

    public static Configuration loadFromResources(final String name) {
        try (InputStream is = FireflyGraph.class.getClassLoader().getResourceAsStream(name)) {
            if (is == null) throw new RuntimeException("unable to find resource " + name);
            try (final InputStreamReader isr = new InputStreamReader(is);
                 final BufferedReader reader = new BufferedReader(isr)) {
                Properties props = new Properties();
                props.load(reader);
                HashMap<String, Object> configData = new HashMap<>();
                props.keySet().forEach(it -> {
                    configData.put(it.toString().toLowerCase(), props.get(it.toString()));
                });
                return new MapConfiguration(configData);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected static boolean checkInternalKeys(final String key) {
        return Keys.InternalConfigs.keys().contains(key) ||
                Keys.Sets.keys().contains(key) ||
                Keys.Bins.keys().contains(key);
    }

    public static Object getOrDefault(final String key, final Configuration config) {
        final String lowerKey = key.toLowerCase();
        final String upperKey = key.toUpperCase();
        final boolean debugMode = config.containsKey(Keys.DEBUG_MODE_FLAG) && config.getBoolean(Keys.DEBUG_MODE_FLAG);

        if (System.getenv().containsKey(lowerKey) || System.getenv().containsKey(upperKey)) {
            String envConfig = System.getenv(upperKey);
            if (envConfig == null || envConfig.isEmpty()) {
                envConfig = System.getenv(lowerKey);
            }
            if (envConfig != null && !envConfig.isEmpty()) {
                return envConfig;
            }
        } else if (!config.containsKey(lowerKey) && !defaultValues.containsKey(key) && !checkInternalKeys(key)) {
            throw new ConfigurationRuntimeException("no default value available for key: " + lowerKey);
        } else if (config.containsKey(lowerKey)) {
            return config.getString(lowerKey);
        } else if (Keys.InternalConfigs.keys().contains(key)) {
            if (debugMode) {
                return Keys.InternalConfigs.valueOf(key).getValue().english;
            } else {
                return Keys.InternalConfigs.valueOf(key).getValue().numeric;
            }
        } else if (Keys.Sets.keys().contains(key)) {
            final String prefix = PREFIX_MASK.contains(lowerKey) ? "" : getPrefix(config);
            if (debugMode) {
                return prefix + Keys.Sets.valueOf(key).getValue().english;
            } else {
                return prefix + Keys.Sets.valueOf(key).getValue().numeric;
            }
        } else if (Keys.Bins.keys().contains(key)) {
            if (debugMode) {
                return Keys.Bins.valueOf(key).getValue().english;
            } else {
                return Keys.Bins.valueOf(key).getValue().numeric;
            }
        }
        return defaultValues.get(key);
    }

    public static String getOrDefaultString(final String key, Configuration config) {
        final Object value = getOrDefault(key, config);
        if (value != null)
            return value.toString();
        else
            return null;
    }

    public static String getPrefix(Configuration config) {
        return config.containsKey(Keys.GRAPH_ID.toLowerCase()) ? config.get(String.class, Keys.GRAPH_ID.toLowerCase()) + "_" : defaultValues.get(Keys.GRAPH_ID) + "_";
    }

    public static String aerospikeNamespace(Configuration c) {
        return c.get(String.class, Keys.AEROSPIKE_NAMESPACE.toLowerCase());
    }

    public static int aerospikePort(Configuration c) {
        return c.get(Integer.class, Keys.AEROSPIKE_PORT.toLowerCase());
    }

    public static String aerospikeHost(Configuration c) {
        return c.get(String.class, Keys.AEROSPIKE_HOST.toLowerCase());
    }

    public static String dumpDefaults() {
        final Properties props = new Properties();
        props.putAll(defaultValues);
        final StringWriter sw = new StringWriter();
        try {
            props.store(sw, "FireflyGraph Configuration Defaults");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return sw.toString();
    }
}
