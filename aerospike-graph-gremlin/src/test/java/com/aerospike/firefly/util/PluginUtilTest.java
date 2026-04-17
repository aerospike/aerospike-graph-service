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

import com.aerospike.firefly.runtime.PluginInterface;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

public class PluginUtilTest extends AbstractFireflySuite {
    public static class TestPlugin implements PluginInterface {
        public static AtomicLong counter = new AtomicLong(0);

        public static TestPlugin open(Configuration conf) {
            return new TestPlugin();
        }

        @Override
        public Map<String, List<String>> api() {
            return new HashMap<>();
        }

        @Override
        public void plugInto(final Object system) {
            if (FireflyGraph.class.isAssignableFrom(system.getClass())) {
                ((FireflyGraph) system).writeGraphVariable("test", "test");
            }
            counter.incrementAndGet();
        }
    }

    @Test
    public void testPlugin() {
        final Configuration pluginConf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        pluginConf.setProperty(ConfigurationHelper.Keys.PLUGIN, TestPlugin.class.getName());
        final long before = TestPlugin.counter.get();
        PluginUtil.loadPlugin(TestPlugin.class.getName(), pluginConf, new Object());
        assertEquals(before + 1, TestPlugin.counter.get());
    }

    @Test
    public void testPlugIntoGraph() throws Exception {
        final Configuration pluginConf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        pluginConf.setProperty(ConfigurationHelper.Keys.PLUGIN, TestPlugin.class.getName());
        final long before = TestPlugin.counter.get();
        final Graph pluggedGraph = FireflyGraph.open(pluginConf);
        assertEquals(before + 1, TestPlugin.counter.get());
        assertEquals("test", pluggedGraph.variables().get("test").get().toString());
        pluggedGraph.close();
    }

    @Override
    protected boolean clearData() {
        return true;
    }
}
