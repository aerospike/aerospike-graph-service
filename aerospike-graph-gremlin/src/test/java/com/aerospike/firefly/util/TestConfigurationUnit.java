package com.aerospike.firefly.util;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestConfigurationUnit {
    @Test
    public void testLoadConfigurationFromResources() {
        final Configuration c = ConfigurationHelper.loadFromResources("integration-test-settings.properties");
        assertEquals(ConfigurationHelper.aerospikeHost(c).getClass(), String.class);
        assertNotEquals(ConfigurationHelper.aerospikePort(c), 0);
        assertEquals(ConfigurationHelper.aerospikeNamespace(c).getClass(), String.class);
        assertFalse(ConfigurationHelper.aerospikeHost(c).isEmpty());
        assertFalse(ConfigurationHelper.aerospikeNamespace(c).isEmpty());
    }

    //this test only works if executed from project source code root
    @Test
    public void testLoadConfigurationFromFile() {
        String here = this.getClass().getProtectionDomain().getCodeSource().getLocation().getPath();
        Configuration c = ConfigurationHelper.loadFromFile(Paths.get(String.format("%s/%s", here, "integration-test-settings.properties")));
        assertEquals(ConfigurationHelper.aerospikePort(c), 3000);
        assertEquals(ConfigurationHelper.aerospikeNamespace(c), "test");
    }

    @Test
    public void testConfigToString() {
        String s = ConfigurationHelper.dumpDefaults();
        assertTrue(s.contains(ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.AEROSPIKE_HOST, new MapConfiguration(new HashMap<>()))));
        assertTrue(s.contains(ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.AEROSPIKE_PORT, new MapConfiguration(new HashMap<>()))));
        assertTrue(s.contains(ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.AEROSPIKE_NAMESPACE, new MapConfiguration(new HashMap<>()))));
    }


    @Test
    public void returnsNormalNameInDebugMode() {
        final Configuration conf = ConfigurationHelper.loadFromResources("integration-test-settings.properties");
        conf.setProperty(ConfigurationHelper.Keys.DEBUG_MODE_FLAG, "true");
        assertEquals(ConfigurationHelper.Keys.Bins.PROPERTIES_BIN.name(),
                ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.PROPERTIES_BIN.name(), conf));
    }

    @Test
    public void returnNumericNameInNormalMode() {
        final Configuration conf = ConfigurationHelper.loadFromResources("integration-test-settings.properties");
        conf.setProperty(ConfigurationHelper.Keys.DEBUG_MODE_FLAG, "false");
        assertEquals(String.valueOf(ConfigurationHelper.Keys.Bins.PROPERTIES_BIN.getValue()),
                ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.PROPERTIES_BIN.name(), conf));
    }
}
