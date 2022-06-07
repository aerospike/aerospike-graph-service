package com.aerospike.firefly.util;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationRuntimeException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class ConfigurationHelper {
    public static class Keys {
        public static final String AEROSPIKE_HOST = "AEROSPIKE_HOST";
        public static final String AEROSPIKE_PORT = "AEROSPIKE_PORT";
        public static final String AEROSPIKE_NAMESPACE = "AEROSPIKE_NAMESPACE";
        public static final String GRAPH_METADATA_SET = "GRAPH_METADATA_SET";
        public static final String GRAPH_VARIABLES_SET = "GRAPH_VARIABLES_SET";
        public static final String GRAPH_VARIABLES_RECORD = "GRAPH_VARIABLES_RECORD";
        public static final String GRAPH_VARIABLES_MAP = "GRAPH_VARIABLES_MAP";
        public static final String EDGE_AERO_SET = "EDGE_AERO_SET";
        public static final String VERTEX_AERO_SET = "VERTEX_AERO_SET";
        public static final String VERTEX_EDGELIST_AERO_SET = "VERTEX_EDGELIST_AERO_SET";
        public static final String PROPERTY_AERO_SET = "PROPERTY_AERO_SET";
        public static final String VERTEX_PROPERTY_AERO_SET = "VERTEX_PROPERTY_AERO_SET";
        public static final String EDGE_ID_KEY = "EDGE_ID_KEY";
        public static final String EDGE_ID_BIN = "EDGE_ID_BIN";
        public static final String VERTEX_ID_KEY = "VERTEX_ID_KEY";
        public static final String VERTEX_ID_BIN = "VERTEX_ID_BIN";
        public static final String VERTEX_PROPERTY_ID_KEY = "VERTEX_PROPERTY_ID_KEY";
        public static final String VERTEX_PROPERTY_ID_BIN = "VERTEX_PROPERTY_ID_BIN";
        public static final String VERTEX_PROPERTY_NAME_TO_ID = "VERTEX_PROPERTY_NAME_TO_ID";
        public static final String VERTEX_PROPERTY_NAME = "VERTEX_PROPERTY_NAME";
        public static final String PARENT_VERTEX_ID = "PARENT_VERTEX_ID";
        public static final String VERTEX_PROPERTY_SET = "VERTEX_PROPERTY_SET";
        public static final String EDGE_PROPERTIES = "EDGE_PROPERTIES";
        public static final String VP_PROPERTIES = "VP_PROPERTIES";
        public static final String TYPE_HINTS = "TYPE_HINTS";
        public static final String KEY_VALUE = "KEY_VALUE";
        public static final String COUNTER = "COUNTER";
        public static final String ID_MANAGER_SET = "ID_MANAGER_SET";
        public static final String ID_TYPE = "ID_TYPE";
        public static final String GLOBAL = "GLOBAL";
        public static final String TEST_SET = "TEST_SET";
    }

    private static final Map<String, String> defaultValues = new HashMap<>() {{
        put(Keys.GRAPH_METADATA_SET, "G_METADATA");
        put(Keys.GRAPH_VARIABLES_SET, "G_VARIABLES");
        put(Keys.GRAPH_VARIABLES_RECORD, "G_VARIABLES_REC");
        put(Keys.GRAPH_VARIABLES_MAP, "G_VARIABLES_MAP");
        put(Keys.EDGE_AERO_SET, "EDGE");
        put(Keys.EDGE_PROPERTIES, "E_PROPERTIES");
        put(Keys.VERTEX_AERO_SET, "VERTEX");
        put(Keys.VERTEX_EDGELIST_AERO_SET, "EDGELIST");
        put(Keys.PROPERTY_AERO_SET, "PROPERTY");
        put(Keys.VERTEX_PROPERTY_AERO_SET, "V_PROPERTY");
        put(Keys.EDGE_ID_KEY, "EDGE_ID_KEY");
        put(Keys.EDGE_ID_BIN, "EDGE_ID_BIN");
        put(Keys.VERTEX_ID_KEY, "VERTEX_ID_KEY");
        put(Keys.VERTEX_ID_BIN, "VERTEX_ID_BIN");
        put(Keys.VERTEX_PROPERTY_SET, "VP");
        put(Keys.VERTEX_PROPERTY_ID_KEY, "VP_ID_KEY");
        put(Keys.VERTEX_PROPERTY_ID_BIN, "VP_P_ID_BIN");
        put(Keys.VERTEX_PROPERTY_NAME_TO_ID, "VP_NAME_ID");
        put(Keys.VERTEX_PROPERTY_NAME, "VP_NAME");
        put(Keys.VP_PROPERTIES, "VP_PROPERTIES");
        put(Keys.TYPE_HINTS,"TYPE_HINTS");
        put(Keys.KEY_VALUE,"KEY_VALUE");
        put(Keys.PARENT_VERTEX_ID, "PARENT_V_ID");
        put(Keys.COUNTER,"COUNTER");
        put(Keys.ID_TYPE,"ID_TYPE");
        put(Keys.ID_MANAGER_SET,"ID_MGR_SET");
        put(Keys.GLOBAL,"GLOBAL");
        put(Keys.TEST_SET,"TEST_SET");
    }};

    public static Configuration loadFromFile(final Path path) {
        try {
            Properties props = new Properties();
            props.load(Files.newBufferedReader(path));
            HashMap<String, Object> configData = new HashMap<>();
            props.keySet().forEach(it -> {
                configData.put(it.toString().toUpperCase(), props.get(it.toString()));
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
                    configData.put(it.toString().toUpperCase(), props.get(it.toString()));
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
            put(Keys.AEROSPIKE_HOST, System.getenv(Keys.AEROSPIKE_HOST));
            put(Keys.AEROSPIKE_PORT, Integer.valueOf(System.getenv(Keys.AEROSPIKE_PORT)));
            put(Keys.AEROSPIKE_NAMESPACE, System.getenv(Keys.AEROSPIKE_NAMESPACE));
        }});
    }

    public static String getOrDefault(final String key, Configuration config) {
        if (!config.containsKey(key) && !defaultValues.containsKey(key))
            throw new ConfigurationRuntimeException("no default value available for key: " + key);
        return config.containsKey(key) ? config.get(String.class, key) : defaultValues.get(key);
    }

    public static String aerospikeNamespace(Configuration c) {
        return c.get(String.class, Keys.AEROSPIKE_NAMESPACE);
    }

    public static int aerospikePort(Configuration c) {
        return c.get(Integer.class, Keys.AEROSPIKE_PORT);
    }

    public static String aerospikeHost(Configuration c) {
        return c.get(String.class, Keys.AEROSPIKE_HOST);
    }

}
