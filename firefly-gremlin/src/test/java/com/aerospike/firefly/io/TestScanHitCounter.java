package com.aerospike.firefly.io;

import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class TestScanHitCounter extends AbstractFireflySuite {
    @Test
    public void testScanHitCounter() {
        ScanHitCounter shc = ScanHitCounter.create();
        shc.increment("a");
        shc.increment("a");
        shc.increment("b");
        shc.increment("c");
        shc.increment("d");
        shc.increment("e");
        assertEquals(5, shc.hitCount.size());
        assertTrue(shc.hitCount.containsKey("a"));
        shc.increment("a");
        shc.increment("a");
        assertEquals(4, shc.get("a"));
    }

    @Test
    public void testThreadLocalScanHitCounter() {
        GraphTraversalSource g = graph.traversal();
        g.addV("lemon").property("color", "yellow").property("type", "plant").next();
        g.addV("lime").property("color", "green").property("type", "plant").next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        TraversalMetrics p = g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("this", "that").profile().next();
        Collection<? extends Metrics> m = p.getMetrics();
        Metrics fm = (Metrics)p.getMetrics().toArray()[4];
        System.out.println(p);
        Metrics nested = fm.getNested("FireflyMetrics");
        assertEquals(3, nested.getAnnotations().size());
    }


    @Override
    protected boolean clearData() {
        return false;
    }
}
