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

package com.aerospike.firefly.util;

import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.MapConfiguration;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

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
        assertEquals(ConfigurationHelper.Keys.Bins.PROPERTIES_BIN.getValue().english,
                ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.PROPERTIES_BIN.name(), conf));
    }

    @Test
    public void returnNumericNameInNormalMode() {
        final Configuration conf = ConfigurationHelper.loadFromResources("integration-test-settings.properties");
        conf.setProperty(ConfigurationHelper.Keys.DEBUG_MODE_FLAG, "false");
        assertEquals(String.valueOf(ConfigurationHelper.Keys.Bins.PROPERTIES_BIN.getValue().numeric),
                ConfigurationHelper.getOrDefaultString(ConfigurationHelper.Keys.Bins.PROPERTIES_BIN.name(), conf));
    }

    @Test
    public void usageStatsEnabledDefaultsToTrue() {
        final Configuration conf = new MapConfiguration(new HashMap<>());
        assertTrue(
                "USAGE_STATS_ENABLED must default to true so existing deployments "
                        + "keep emitting local self-instrumentation when the new key is absent.",
                ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.USAGE_STATS_ENABLED, conf));
    }

    @Test
    public void usageStatsEnabledCanBeDisabled() {
        final Configuration conf = new MapConfiguration(new HashMap<>());
        conf.setProperty(ConfigurationHelper.Keys.USAGE_STATS_ENABLED, "false");
        assertFalse(
                "Setting USAGE_STATS_ENABLED=false must disable the local usage-stats writer.",
                ConfigurationHelper.getOrDefaultBool(ConfigurationHelper.Keys.USAGE_STATS_ENABLED, conf));
    }
}
