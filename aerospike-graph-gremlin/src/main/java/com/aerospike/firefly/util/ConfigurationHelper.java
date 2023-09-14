package com.aerospike.firefly.util;

import com.aerospike.firefly.io.AerospikeConnection;
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

import static com.aerospike.firefly.io.AerospikeConnection.getDefaultThreadPoolSize;

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
        public static final String VERTEX_PROPERTY_INDEXES = "aerospike.graph.index.vertex.properties";
        public static final String EDGE_PROPERTY_INDEXES = "aerospike.graph.index.edge.properties";
        public static final String GRAPH_ID = "aerospike.graph.id";
        public static final String PROMETHEUS_PORT = "aerospike.graph.prometheus.port";
        public static final String PROMETHEUS_PATH = "aerospike.graph.prometheus.path";
        public static final String PLUGIN = "aerospike.graph.plugin";
        
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
        public static final String ENABLE_BATCH_EDGE_READ_STRATEGY = "aerospike.graph.strategy.batch.edge.read.enabled";
        public static final String ENABLE_EMBEDDED_BATCH_EDGE_READ_STRATEGY = "aerospike.graph.strategy.batch.edge.read.embedded.enabled";
        public static final String GLOBAL_EDGE_CACHE_ENABLED = "aerospike.graph.global.edge.cache.enabled";
        public static final String VERTEX_ID_BUFFER_SIZE = "aerospike.graph.vertex.id.buffer.size";
        public static final String EDGE_ID_BUFFER_SIZE = "aerospike.graph.edge.id.buffer.size";
        public static final String PROPERTY_ID_BUFFER_SIZE = "aerospike.graph.property.id.buffer.size";
        public static final String STORAGE_DEBUGGER_FLAG = "storage.debug";

        // TODO: Once we are 100% sure these are stable, we can remove the enable flags.
        public static final String ENABLE_EMBEDDED_GRAPH_COUNT_STRATEGY = "aerospike.graph.strategy.fast.count.embedded.enabled";
        public static final String ENABLE_EMBEDDED_VERTEX_EDGE_LOCAL_COUNT_STRATEGY = "aerospike.graph.strategy.local.fast.count.embedded.enabled";
        public static final String ENABLE_BATCHED_REPEAT_STEP_STRATEGY = "aerospike.graph.strategy.batched.repeat.step.enabled";
        public static final String ENABLE_EMBEDDED_LOCAL_COUNT_STRATEGY = "aerospike.graph.strategy.local.count.embedded.enabled";

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

        public static final String CLIENT_FAILURE_TEST = "aerospike.graph.failure.client.enabled";
        public static final String CLIENT_FAILURE_RATE = "aerospike.graph.failure.client.rate";
        public static final String MAX_ERROR_RATE = "aerospike.client.maxErrorRate";
        public static final String MAX_CONNECTIONS_PER_NODE = "aerospike.client.maxConnectionsPerNode";
        public static final String MIN_CONNECTIONS_PER_NODE = "aerospike.client.minConnectionsPerNode";
        public static final String CONNECT_TIMEOUT = "aerospike.client.connectTimeout";
        public static final String TIMEOUT_DELAY = "aerospike.client.timeoutDelay";

        public enum Bins {
            GRAPH_VARIABLES_BIN((byte) 1),
            VERTEX_PROPERTY_NAME_TO_VALUE_BIN((byte) 2),
            VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN((byte) 3),
            RELATIONAL_VERTEX_TYPE_HINT_BIN((byte) 4),
            EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN((byte) 5),
            EDGE_CACHE_DISABLED_BIN((byte) 6),
            IN_EDGES_BIN((byte) 7),
            OUT_EDGES_BIN((byte) 8),
            PROPERTIES_BIN((byte) 9),
            TYPE_HINTS_BIN((byte) 10),
            COUNTER_BIN((byte) 11),
            ID_TYPE_BIN((byte) 12),
            USER_KEY_BIN((byte) 13),
            LABEL_BIN((byte) 14),
            IN_EDGE_COUNTER_BIN((byte) 15),
            OUT_EDGE_COUNTER_BIN((byte) 16),
            VERTEX_PROPERTY_NAME_TO_ID_BIN((byte) 17);

            private final byte value;

            Bins(byte b) {
                this.value = b;
            }

            public byte getValue() {
                return value;
            }

            public static Set<String> keys() {
                return Arrays.stream(Bins.values()).map(Bins::name).collect(Collectors.toSet());
            }
        }

        public enum InternalConfigs {
            GRAPH_VARIABLES_REC_KEY((byte) 0),
            VERTEX_PROPERTY_NAME((byte) 3),
            V_LABEL_INDEX_NAME((byte) 4),
            E_LABEL_INDEX_NAME((byte) 5),
            E_IN_INDEX_NAME((byte) 6),
            E_OUT_INDEX_NAME((byte) 7),
            VP_PROPERTIES((byte) 8),
            VP_TYPE_HINTS((byte) 9),
            GLOBAL((byte) 10),
            SUPERNODES_IN((byte) 11),
            SUPERNODES_OUT((byte) 12),
            INDEX_METADATA_SET((byte) 13),
            LABEL((byte) 14),
            USER_SUPPLIED_ID_VERTEX_CACHE((byte) 15),
            USER_SUPPLIED_ID_EDGE_CACHE((byte) 16),
            USER_SUPPLIED_ID_VERTEX_PROPERTY_CACHE((byte) 17);

            private final byte value;

            private InternalConfigs(byte value) {
                this.value = value;
            }

            public int getValue() {
                return value;
            }

            public static Set<String> keys() {
                return Arrays.stream(InternalConfigs.values()).map(InternalConfigs::name).collect(Collectors.toSet());
            }

        }

        public enum Sets {
            GRAPH_VARIABLES_SET((byte) 0),
            EDGE_AERO_SET((byte) 1),
            VERTEX_AERO_SET((byte) 2),
            IN_VP_SET((byte) 3),
            OUT_VP_SET((byte) 4),
            IN_IN_SET((byte) 5),
            IN_OUT_SET((byte) 6),
            OUT_IN_SET((byte) 7),
            OUT_OUT_SET((byte) 8),
            ID_MANAGER_SET((byte) 9),
            SUMMARY_SET((byte) 10),
            TEST_SET((byte) 11),
            GRAPH_METADATA_SET((byte) 12),
            USER_SUPPLIED_ID_CACHE_SET((byte) 30);

            private final byte value;

            public static Set<String> keys() {
                return Arrays.stream(Sets.values()).map(Sets::name).collect(Collectors.toSet());
            }

            public byte getValue() {
                return value;
            }

            Sets(byte b) {
                this.value = b;
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

        put(Keys.InternalConfigs.GRAPH_VARIABLES_REC_KEY.name(), "G_VAR_REC");
        put(Keys.Bins.GRAPH_VARIABLES_BIN.name(), "G_VAR_MAP");
        put(Keys.Bins.PROPERTIES_BIN.name(), "PROPERTIES");
        put(Keys.Bins.VERTEX_PROPERTY_NAME_TO_ID_BIN.name(), "VP_N_ID");
        put(Keys.Bins.VERTEX_PROPERTY_NAME_TO_VALUE_BIN.name(), "VP_N_V");
        put(Keys.Bins.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT_BIN.name(), "VP_N_TH");
        put(Keys.InternalConfigs.VERTEX_PROPERTY_NAME.name(), "VP_NAME");
        put(Keys.Bins.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN.name(), "E_L_E_L_E");
        put(Keys.InternalConfigs.VP_PROPERTIES.name(), "VP_PROP");
        put(Keys.Bins.TYPE_HINTS_BIN.name(), "TYPE_HINTS");
        put(Keys.InternalConfigs.VP_TYPE_HINTS.name(), "VP_TYPE_HINTS");
        put(Keys.Bins.COUNTER_BIN.name(), "COUNTER");
        put(Keys.Bins.ID_TYPE_BIN.name(), "ID_TYPE");
        put(Keys.InternalConfigs.GLOBAL.name(), "GLOBAL");
        put(Keys.Bins.IN_EDGE_COUNTER_BIN.name(), "IN_E_CTR");
        put(Keys.Bins.OUT_EDGE_COUNTER_BIN.name(), "OUT_E_CTR");
        put(Keys.Bins.IN_EDGES_BIN.name(), "IN_EDGES");
        put(Keys.Bins.OUT_EDGES_BIN.name(), "OUT_EDGES");
        put(Keys.Bins.EDGE_CACHE_DISABLED_BIN.name(), "CACHE_DISABLED");
        put(Keys.InternalConfigs.INDEX_METADATA_SET.name(), "INDEX_META");
        put(Keys.Bins.RELATIONAL_VERTEX_TYPE_HINT_BIN.name(), "V_TYP_HNT");
        put(Keys.InternalConfigs.V_LABEL_INDEX_NAME.name(), "V_LABEL_IDX");
        put(Keys.InternalConfigs.E_LABEL_INDEX_NAME.name(), "E_LABEL_IDX");
        put(Keys.InternalConfigs.E_IN_INDEX_NAME.name(), "E_IN_INDEX");
        put(Keys.InternalConfigs.E_OUT_INDEX_NAME.name(), "E_OUT_INDEX");
        put(Keys.Sets.USER_SUPPLIED_ID_CACHE_SET.name(), "USER_SUPPLIED_ID_CACHE_SET");
        put(Keys.InternalConfigs.USER_SUPPLIED_ID_VERTEX_CACHE.name(), "USER_SUPPLIED_ID_VERTEX_CACHE");
        put(Keys.InternalConfigs.USER_SUPPLIED_ID_EDGE_CACHE.name(), "USER_SUPPLIED_ID_EDGE_CACHE");
        put(Keys.InternalConfigs.USER_SUPPLIED_ID_VERTEX_PROPERTY_CACHE.name(), "USER_SUPPLIED_ID_VERTEX_PROPERTY_CACHE");
        put(Keys.InternalConfigs.SUPERNODES_IN.name(), "SUPERNODES_IN");
        put(Keys.InternalConfigs.SUPERNODES_OUT.name(), "SUPERNODES_OUT");

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
        put(Keys.ENABLE_BATCH_EDGE_READ_STRATEGY, "true");
        put(Keys.ENABLE_EMBEDDED_BATCH_EDGE_READ_STRATEGY, "true");
        put(Keys.ENABLE_EMBEDDED_GRAPH_COUNT_STRATEGY, "true");
        put(Keys.ENABLE_EMBEDDED_VERTEX_EDGE_LOCAL_COUNT_STRATEGY, "true");
        put(Keys.ENABLE_BATCHED_REPEAT_STEP_STRATEGY, "true");
        put(Keys.ENABLE_EMBEDDED_LOCAL_COUNT_STRATEGY, "true");
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
        put(Keys.Sets.SUMMARY_SET.name(), "G_SUMMARY");
        put(Keys.Sets.ID_MANAGER_SET.name(), "ID_MGR_SET");
        put(Keys.Sets.GRAPH_METADATA_SET.name(), "G_META");
        put(Keys.Sets.GRAPH_VARIABLES_SET.name(), "G_VAR");
        put(Keys.Sets.EDGE_AERO_SET.name(), "EDGE");
        put(Keys.Sets.VERTEX_AERO_SET.name(), "VERTEX");
        put(Keys.Sets.IN_VP_SET.name(), "IN_VP");
        put(Keys.Sets.OUT_VP_SET.name(), "OUT_VP");
        put(Keys.Sets.IN_IN_SET.name(), "IN_IN");
        put(Keys.Sets.IN_OUT_SET.name(), "IN_OUT");
        put(Keys.Sets.OUT_IN_SET.name(), "OUT_IN");
        put(Keys.Sets.OUT_OUT_SET.name(), "OUT_OUT");
        put(Keys.Sets.TEST_SET.name(), "TEST_SET");
        put(Keys.BULK_LOADER_FLAG, "false");
        put(Keys.MAX_ERROR_RATE, "100");
        put(Keys.MAX_CONNECTIONS_PER_NODE, String.valueOf(getDefaultThreadPoolSize(FireflyGraph.getGremlinServerSettings()) * 2));
        put(Keys.MIN_CONNECTIONS_PER_NODE, String.valueOf(getDefaultThreadPoolSize(FireflyGraph.getGremlinServerSettings())));
        put(Keys.CONNECT_TIMEOUT, "0");
        put(Keys.TIMEOUT_DELAY, "0");
        put(Keys.DEBUG_MODE_FLAG, "false");
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
                return Keys.InternalConfigs.valueOf(key).name();
            } else {
                return Keys.InternalConfigs.valueOf(key).getValue();
            }
        } else if (Keys.Sets.keys().contains(key)) {
            final String prefix = PREFIX_MASK.contains(lowerKey) ? "" : getPrefix(config);
            if (debugMode) {
                return prefix + Keys.Sets.valueOf(key).name();
            } else {
                return prefix + Keys.Sets.valueOf(key).getValue();
            }
        } else if (Keys.Bins.keys().contains(key)) {
            if (debugMode) {
                return Keys.Bins.valueOf(key).name();
            } else {
                return Keys.Bins.valueOf(key).getValue();
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

    private static String getPrefix(Configuration config) {
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
