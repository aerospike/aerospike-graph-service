package com.aerospike.firefly.util;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

    }



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
        try (InputStream is = ConfigurationHelper.class.getClassLoader().getResourceAsStream(name)) {
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
        return new MapConfiguration(new HashMap<>() {{
            put(Keys.AEROSPIKE_HOST, System.getenv(Keys.AEROSPIKE_HOST));
            put(Keys.AEROSPIKE_PORT, Integer.valueOf(System.getenv(Keys.AEROSPIKE_PORT)));
            put(Keys.AEROSPIKE_NAMESPACE, System.getenv(Keys.AEROSPIKE_NAMESPACE));
        }});
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
