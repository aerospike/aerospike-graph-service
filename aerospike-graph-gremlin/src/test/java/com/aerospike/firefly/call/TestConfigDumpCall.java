package com.aerospike.firefly.call;

import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class TestConfigDumpCall extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void testExecution() throws InterruptedException {
        final GraphTraversalSource g = graph.traversal();
        g.V().drop().iterate();
        Thread.sleep(100);
        final Object callResult = g.call("aerospike.graph.admin.config.dump-defaults").next();
        final Map<Object, String> expected = ConfigurationHelper.dumpDefaultsMap();
        Assert.assertNotEquals(0, expected.size());
        Assert.assertEquals(expected, callResult);
    }

    @Test
    public void testWrongConfig() throws InterruptedException {
        final GraphTraversalSource g = graph.traversal();
        g.V().drop().iterate();
        Thread.sleep(100);
        Assert.assertThrows(IllegalArgumentException.class,
                () -> g.call("aerospike.graph.admin.config.dump-defaults")
                        .with("Bombo").with("Rass", "Clart").next());
    }
}

