package com.aerospike.firefly.util;

import com.aerospike.firefly.runtime.PluginInterface;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static org.junit.Assert.assertEquals;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
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
    public void testPlugIntoGraph() {
        final Configuration pluginConf = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        pluginConf.setProperty(ConfigurationHelper.Keys.PLUGIN, TestPlugin.class.getName());
        final long before = TestPlugin.counter.get();
        final Graph pluggedGraph = FireflyGraph.open(pluginConf);
        assertEquals(before + 1, TestPlugin.counter.get());
        assertEquals("test", pluggedGraph.variables().get("test").get().toString());
    }

    @Override
    protected boolean clearData() {
        return true;
    }
}
