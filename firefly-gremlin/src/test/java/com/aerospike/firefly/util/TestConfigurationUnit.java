package com.aerospike.firefly.util;

import org.apache.commons.configuration2.Configuration;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestConfigurationUnit {


    @Test
    void testLoadConfigurationFromResources() {
        final Configuration c = ConfigurationHelper.loadFromResources("phaseshift-integration-settings.properties");
        assertEquals(ConfigurationHelper.aerospikeHost(c).getClass(), String.class);
        assertNotEquals(ConfigurationHelper.aerospikePort(c), 0);
        assertEquals(ConfigurationHelper.aerospikeNamespace(c).getClass(), String.class);
        assertFalse(ConfigurationHelper.aerospikeHost(c).isEmpty());
        assertFalse(ConfigurationHelper.aerospikeNamespace(c).isEmpty());
    }

    //this test only works if executed from project source code root
    @Test
    void testLoadConfigurationFromFile() {
        String here = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        Configuration c = ConfigurationHelper.loadFromFile(Paths.get(String.format("%s/%s", here, "phaseshift-integration-settings.properties")));
        assertEquals(ConfigurationHelper.aerospikePort(c), 3000);
        assertEquals(ConfigurationHelper.aerospikeNamespace(c), "test");
    }

    private static Map<String, String> getModifiableEnvironment() throws Exception {
        Class pe = Class.forName("java.lang.ProcessEnvironment");
        Method getenv = pe.getDeclaredMethod("getenv");
        getenv.setAccessible(true);
        Object unmodifiableEnvironment = getenv.invoke(null);
        Class map = Class.forName("java.util.Collections$UnmodifiableMap");
        Field m = map.getDeclaredField("m");
        m.setAccessible(true);
        return (Map) m.get(unmodifiableEnvironment);
    }

    @Test
    void testLoadConfigurationFromEnv() throws Exception {
        Map<String, String> env = getModifiableEnvironment();
        env.put(ConfigurationHelper.Keys.AEROSPIKE_NAMESPACE, "test");
        env.put(ConfigurationHelper.Keys.AEROSPIKE_HOST, "aerospike-dev.phaseshift.internal");
        env.put(ConfigurationHelper.Keys.AEROSPIKE_PORT, "3000");
        final Configuration c = ConfigurationHelper.loadFromEnv();
        assertEquals(ConfigurationHelper.aerospikePort(c), 3000);
        assertEquals(ConfigurationHelper.aerospikeNamespace(c), "test");
    }
}
