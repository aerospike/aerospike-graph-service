package com.aerospike.firefly.util;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyConfiguration {
    private static class Keys {
        public static final String AEROSPIKE_HOST = "AEROSPIKE_HOST";
        public static final String AEROSPIKE_PORT = "AEROSPIKE_PORT";
        public static final String AEROSPIKE_NAMESPACE = "AEROSPIKE_NAMESPACE";

    }

    private final Map<String, Object> data;

    private FireflyConfiguration(final Map<String, Object> data) {
        this.data = data;
    }

    public static FireflyConfiguration loadFromFile(final String path) {
        try {
            Properties props = new Properties();
            props.load(Files.newBufferedReader(Paths.get(path)));
            return new FireflyConfiguration(new HashMap(props));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static FireflyConfiguration loadFromResources(final String name) {
        try (InputStream is = FireflyConfiguration.class.getClassLoader().getResourceAsStream(name)) {
            if (is == null) throw new RuntimeException("unable to find resource " + name);
            try (final InputStreamReader isr = new InputStreamReader(is);
                 final BufferedReader reader = new BufferedReader(isr)) {
                Properties props = new Properties();
                props.load(reader);
                return new FireflyConfiguration(new HashMap(props));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static FireflyConfiguration loadFromEnv() {
        return new FireflyConfiguration(new HashMap<>() {{
            put(Keys.AEROSPIKE_HOST, System.getenv(Keys.AEROSPIKE_HOST));
            put(Keys.AEROSPIKE_PORT, System.getenv(Keys.AEROSPIKE_PORT));
        }});
    }

    public Configuration toApacheConfiguration() {
        BaseConfiguration c = new BaseConfiguration();
        data.forEach(c::setProperty);
        return c;
    }


    public String aerospikeHost() {
        return (String) data.get(Keys.AEROSPIKE_HOST.toLowerCase());
    }

    public int aerospikePort() {
        return Integer.valueOf((String) data.get(Keys.AEROSPIKE_PORT.toLowerCase()));
    }

    public String aerospikeNamespace() {
        return (String) data.get(Keys.AEROSPIKE_NAMESPACE.toLowerCase());
    }
}
