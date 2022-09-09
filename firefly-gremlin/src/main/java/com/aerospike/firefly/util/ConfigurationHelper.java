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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public final class ConfigurationHelper {
    private static final Logger logger = LoggerFactory.getLogger(AerospikeConnection.class);

    private ConfigurationHelper() {
    }

    private static List PREFIX_MASK = new ArrayList() {{
        add(Keys.GRAPH_ID);
        add(Keys.ID_CACHE_SIZE);
    }};

    public static class Keys {


        public static class Sets {
            public static final String GRAPH_VARIABLES_SET = "GRAPH_VARIABLES_SET";
            public static final String EDGE_AERO_SET = "EDGE_AERO_SET";
            public static final String VERTEX_AERO_SET = "VERTEX_AERO_SET";
            public static final String IN_VP_SET = "IN_VP_SET";
            public static final String OUT_VP_SET = "OUT_VP_SET";
            public static final String IN_IN_SET = "IN_IN_SET";
            public static final String IN_OUT_SET = "IN_OUT_SET";
            public static final String OUT_IN_SET = "OUT_IN_SET";
            public static final String OUT_OUT_SET = "OUT_OUT_SET";
            public static final String VERTEX_EDGELIST_AERO_SET = "VERTEX_EDGELIST_AERO_SET";
            public static final String VERTEX_PROPERTY_AERO_SET = "VERTEX_PROPERTY_AERO_SET";
            public static final String VERTEX_PROPERTY_SET = "VERTEX_PROPERTY_SET";
            public static final String ID_MANAGER_SET = "ID_MANAGER_SET";
            public static final String TEST_SET = "TEST_SET";
            public static final String GRAPH_METADATA_SET = "GRAPH_METADATA_SET";

        }

        public static final String FIREFLY_DATA_MODEL = "FIREFLY_DATA_MODEL";
        public static final String ASYNC_SUBGRAPH_CACHE = "ASYNC_SUBGRAPH_CACHE";
        public static final String SCAN_MAX_WAIT = "SCAN_MAX_WAIT";
        public static final String AEROSPIKE_HOST = "AEROSPIKE_HOST";
        public static final String AEROSPIKE_PORT = "AEROSPIKE_PORT";
        public static final String AEROSPIKE_NAMESPACE = "AEROSPIKE_NAMESPACE";
        public static final String GRAPH_VARIABLES_RECORD = "GRAPH_VARIABLES_RECORD";
        public static final String GRAPH_VARIABLES_MAP = "GRAPH_VARIABLES_MAP";
        public static final String EDGE_ID_KEY = "EDGE_ID_KEY";
        public static final String EDGE_ID_BIN = "EDGE_ID_BIN";
        public static final String VERTEX_ID_KEY = "VERTEX_ID_KEY";
        public static final String VERTEX_ID_BIN = "VERTEX_ID_BIN";
        public static final String VERTEX_PROPERTY_ID_KEY = "VERTEX_PROPERTY_ID_KEY";
        public static final String VERTEX_PROPERTY_ID_BIN = "VERTEX_PROPERTY_ID_BIN";
        public static final String VERTEX_PROPERTY_NAME_TO_ID = "VERTEX_PROPERTY_NAME_TO_ID";
        public static final String VERTEX_PROPERTY_NAME_TO_VALUE = "VERTEX_PROPERTY_NAME_TO_VALUE";
        public static final String VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT = "VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT";
        public static final String VERTEX_PROPERTY_NAME = "VERTEX_PROPERTY_NAME";
        public static final String EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN = "EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN";
        public static final String PARENT_VERTEX_ID = "PARENT_VERTEX_ID";
        public static final String EDGE_PROPERTIES = "EDGE_PROPERTIES";
        public static final String VP_PROPERTIES = "VP_PROPERTIES";
        public static final String TYPE_HINTS = "TYPE_HINTS";
        public static final String KEY_VALUE = "KEY_VALUE";
        public static final String COUNTER = "COUNTER";
        public static final String ID_TYPE = "ID_TYPE";
        public static final String GLOBAL = "GLOBAL";
        public static final String IN_EDGE_COUNTER = "IN_EDGE_COUNTER";
        public static final String OUT_EDGE_COUNTER = "OUT_EDGE_COUNTER";
        public static final String ID_CACHE_SIZE = "ON_RECORD_ID_LIMIT";
        public static final String VP_COUNTER = "VP_COUNTER";
        public static final String GRAPH_ID = "GRAPH_ID";

        public static final String IN_EDGES = "IN_EDGES";
        public static final String OUT_EDGES = "OUT_EDGES";
        public static final String CACHE_DISABLED = "CACHE_DISABLED";
        public static final String INDEX_METADATA = "INDEX_META";
        public static final String RELATIONAL_VERTEX_TYPE_HINT = "RELATIONAL_VERTEX_TYPE_HINT";

        public static final String NUMERIC_VP_KV_INDEX = "NUMERIC_VP_KV_INDEX";
        public static final String STRING_VP_KV_INDEX = "STRING_VP_KV_INDEX";
        public static final String NUMERIC_V_VP_KV_INDEX = "NUMERIC_V_VP_KV_INDEX";
        public static final String STRING_V_VP_KV_INDEX = "STRING_V_VP_KV_INDEX";
        public static final String STRING_E_KV_INDEX = "STRING_E_KV_INDEX";
        public static final String NUMERIC_E_KV_INDEX = "NUMERIC_E_KV_INDEX";
        public static final String INDEXED_BINS = "INDEXED_BINS";
        public static final String LABEL = "LABEL";
        public static final String V_LABEL_INDEX = "V_LABEL_INDEX";
        public static final String E_LABEL_INDEX = "E_LABEL_INDEX";
        public static final String E_IN_INDEX = "E_IN_INDEX";
        public static final String E_OUT_INDEX = "E_OUT_INDEX";
        // User supplied id cache
        public static final String USER_SUPPLIED_ID_CACHE_SET = "USER_SUPPLIED_ID_CACHE_SET";
        public static final String USER_SUPPLIED_ID_VERTEX_CACHE = "USER_SUPPLIED_ID_VERTEX_CACHE";
        public static final String USER_SUPPLIED_ID_EDGE_CACHE = "USER_SUPPLIED_ID_EDGE_CACHE";
        public static final String USER_SUPPLIED_ID_VERTEX_PROPERTY_CACHE = "USER_SUPPLIED_ID_VERTEX_PROPERTY_CACHE";

        // Disable fast-count by default as current fast count Info() implementation appears to lag under some circumstances
        public static final String ENABLE_FAST_COUNT_STRATEGY = "ENABLE_FAST_COUNT_STRATEGY";
        public static final String ENABLE_SUBGRAPH_CACHE_STRATEGY = "ENABLE_SUBGRAPH_CACHE_STRATEGY";


    }

    private static final Map<String, String> defaultValues = new HashMap<>() {{
        put(Keys.Sets.GRAPH_METADATA_SET, "G_META");
        put(Keys.Sets.GRAPH_VARIABLES_SET, "G_VAR");
        put(Keys.GRAPH_VARIABLES_RECORD, "G_VAR_REC");
        put(Keys.GRAPH_VARIABLES_MAP, "G_VAR_MAP");
        put(Keys.Sets.EDGE_AERO_SET, "EDGE");
        put(Keys.EDGE_PROPERTIES, "E_PROP");
        put(Keys.Sets.VERTEX_AERO_SET, "VERTEX");
        put(Keys.Sets.IN_VP_SET, "IN_VP");
        put(Keys.Sets.OUT_VP_SET, "OUT_VP");
        put(Keys.Sets.IN_IN_SET, "IN_IN");
        put(Keys.Sets.IN_OUT_SET, "IN_OUT");
        put(Keys.Sets.OUT_IN_SET, "OUT_IN");
        put(Keys.Sets.OUT_OUT_SET, "OUT_OUT");
        put(Keys.Sets.VERTEX_EDGELIST_AERO_SET, "E_LIST");
        put(Keys.Sets.VERTEX_PROPERTY_AERO_SET, "V_PROP");
        put(Keys.EDGE_ID_KEY, "E_ID_KEY");
        put(Keys.EDGE_ID_BIN, "E_ID_BIN");
        put(Keys.VERTEX_ID_KEY, "V_ID_KEY");
        put(Keys.VERTEX_ID_BIN, "V_ID_BIN");
        put(Keys.Sets.VERTEX_PROPERTY_SET, "VP");
        put(Keys.VERTEX_PROPERTY_ID_KEY, "VP_ID_K");
        put(Keys.VERTEX_PROPERTY_ID_BIN, "VP_P_ID_B");
        put(Keys.VERTEX_PROPERTY_NAME_TO_ID, "VP_N_ID");
        put(Keys.VERTEX_PROPERTY_NAME_TO_VALUE, "VP_N_V");
        put(Keys.VERTEX_PROPERTY_NAME_TO_VALUE_TYPE_HINT, "VP_N_TH");
        put(Keys.VERTEX_PROPERTY_NAME, "VP_NAME");
        put(Keys.EDGE_LABEL_TO_EDGE_LABEL_TO_EDGES_BIN, "E_L_E_L_E");
        put(Keys.VP_PROPERTIES, "VP_PROP");
        put(Keys.TYPE_HINTS, "TYPE_HINTS");
        put(Keys.KEY_VALUE, "KEY_VALUE");
        put(Keys.PARENT_VERTEX_ID, "PAR_V_ID");
        put(Keys.COUNTER, "COUNTER");
        put(Keys.ID_TYPE, "ID_TYPE");
        put(Keys.Sets.ID_MANAGER_SET, "ID_MGR_SET");
        put(Keys.GLOBAL, "GLOBAL");
        put(Keys.Sets.TEST_SET, "TEST_SET");
        put(Keys.IN_EDGE_COUNTER, "IN_E_CTR");
        put(Keys.OUT_EDGE_COUNTER, "OUT_E_CTR");
        put(Keys.ID_CACHE_SIZE, "100000");
        put(Keys.VP_COUNTER, "VP_COUNT");
        put(Keys.GRAPH_ID, "0");
        put(Keys.IN_EDGES, "IN_EDGES");
        put(Keys.OUT_EDGES, "OUT_EDGES");
        put(Keys.CACHE_DISABLED, "CACHE_DISABLED");
        put(Keys.INDEX_METADATA, "INDEX_META");
        put(Keys.RELATIONAL_VERTEX_TYPE_HINT, "V_TYP_HNT");
        put(Keys.NUMERIC_VP_KV_INDEX, "N_VP_KV");
        put(Keys.STRING_VP_KV_INDEX, "S_VP_KV");
        put(Keys.NUMERIC_V_VP_KV_INDEX, "N_V_VP_KV");
        put(Keys.STRING_V_VP_KV_INDEX, "S_V_VP_KV");
        put(Keys.STRING_E_KV_INDEX, "S_E_KV");
        put(Keys.NUMERIC_E_KV_INDEX, "N_E_KV");
        put(Keys.INDEXED_BINS, "indexedBins");
        put(Keys.V_LABEL_INDEX, "V_LABEL_IDX");
        put(Keys.E_LABEL_INDEX, "E_LABEL_IDX");
        put(Keys.E_IN_INDEX, "E_IN_INDEX");
        put(Keys.E_OUT_INDEX, "E_OUT_INDEX");
        put(Keys.SCAN_MAX_WAIT, "2000");
        // User supplied
        put(Keys.USER_SUPPLIED_ID_CACHE_SET, "USER_SUPPLIED_ID_CACHE_SET");
        put(Keys.USER_SUPPLIED_ID_VERTEX_CACHE, "USER_SUPPLIED_ID_VERTEX_CACHE");
        put(Keys.USER_SUPPLIED_ID_EDGE_CACHE, "USER_SUPPLIED_ID_EDGE_CACHE");
        put(Keys.USER_SUPPLIED_ID_VERTEX_PROPERTY_CACHE, "USER_SUPPLIED_ID_VERTEX_PROPERTY_CACHE");

        put(Keys.ENABLE_FAST_COUNT_STRATEGY, "false");
        put(Keys.ENABLE_SUBGRAPH_CACHE_STRATEGY, "true");
        put(Keys.ASYNC_SUBGRAPH_CACHE, "false");
        put(Keys.AEROSPIKE_PORT, "3000");
    }};

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

    public static Configuration loadFromEnv() {
        ArrayList<String> missingVariables = new ArrayList<>();
        if (System.getenv(Keys.AEROSPIKE_HOST) == null || System.getenv(Keys.AEROSPIKE_HOST).isEmpty())
            missingVariables.add(Keys.AEROSPIKE_HOST);
        if (System.getenv(Keys.AEROSPIKE_PORT) == null || System.getenv(Keys.AEROSPIKE_PORT).isEmpty())
            missingVariables.add(Keys.AEROSPIKE_PORT);
        if (System.getenv(Keys.AEROSPIKE_NAMESPACE) == null || System.getenv(Keys.AEROSPIKE_NAMESPACE).isEmpty())
            missingVariables.add(Keys.AEROSPIKE_NAMESPACE);
        if (!missingVariables.isEmpty())
            throw new RuntimeException("Required environment variable(s) not set: " + missingVariables);


        return new MapConfiguration(new HashMap<>() {{
            put(Keys.AEROSPIKE_HOST.toLowerCase(), System.getenv(Keys.AEROSPIKE_HOST));
            put(Keys.AEROSPIKE_PORT.toLowerCase(), Integer.valueOf(System.getenv(Keys.AEROSPIKE_PORT)));
            put(Keys.AEROSPIKE_NAMESPACE.toLowerCase(), System.getenv(Keys.AEROSPIKE_NAMESPACE));
        }});
    }

    public static String getOrDefault(final String key, Configuration config) {
        final String lowerKey = key.toLowerCase();
        if (!config.containsKey(lowerKey) && !defaultValues.containsKey(key))
            throw new ConfigurationRuntimeException("no default value available for key: " + lowerKey);
        try {
            Keys.Sets.class.getField(key);
        } catch (NoSuchFieldException e) {
            return config.containsKey(lowerKey) ? config.get(String.class, lowerKey) : defaultValues.get(key);
        }
        return (PREFIX_MASK.contains(lowerKey) ? "" : getPrefix(config)) + (config.containsKey(lowerKey) ? config.get(String.class, lowerKey) : defaultValues.get(key));
    }

    private static String getPrefix(Configuration config) {
        return config.containsKey(Keys.GRAPH_ID.toLowerCase()) ? config.get(String.class, Keys.GRAPH_ID.toLowerCase()) : defaultValues.get(Keys.GRAPH_ID) + "_";
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

}
