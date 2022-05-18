package com.aerospike.firefly.util;

import com.aerospike.firefly.util.FireflyConfiguration;
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
        final FireflyConfiguration c = FireflyConfiguration.loadFromResources("phaseshift-integration-settings.properties");
        assertEquals(c.aerospikeHost().getClass(), String.class);
        assertNotEquals(c.aerospikePort(), 0);
        assertEquals(c.aerospikeNamespace().getClass(), String.class);
        assertFalse(c.aerospikeHost().isEmpty());
        assertFalse(c.aerospikeNamespace().isEmpty());
    }

    //this test only works if executed from project source code root
    @Test
    void testLoadConfigurationFromFile() {
        String here = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        FireflyConfiguration c = FireflyConfiguration.loadFromFile(Paths.get(String.format("%s/%s", here, "phaseshift-integration-settings.properties")));
        assertEquals(c.aerospikePort(), 3000);
        assertEquals(c.aerospikeNamespace(), "test");
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
        env.put(FireflyConfiguration.Keys.AEROSPIKE_NAMESPACE, "test");
        env.put(FireflyConfiguration.Keys.AEROSPIKE_HOST, "aerospike-dev.phaseshift.internal");
        env.put(FireflyConfiguration.Keys.AEROSPIKE_PORT, "3000");
        final FireflyConfiguration c = FireflyConfiguration.loadFromEnv();
        assertEquals(c.aerospikePort(), 3000);
        assertEquals(c.aerospikeNamespace(), "test");
    }
}
