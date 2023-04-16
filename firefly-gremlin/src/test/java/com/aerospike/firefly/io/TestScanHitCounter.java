package com.aerospike.firefly.io;

import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.Metrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Edge;
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
        Metrics fm = (Metrics) p.getMetrics().toArray()[4];
        System.out.println(p);
        Metrics nested = fm.getNested("FireflyMetrics");
        assertEquals(3, nested.getAnnotations().size());
    }

    @Test
    public void testWrite2VertexWithEdge() {
        GraphTraversalSource g = graph.traversal();
        Vertex lemon = g.addV("lemon").property("color", "yellow").property("type", "plant").next();
        Vertex lime = g.addV("lime").property("color", "green").property("type", "plant").next();
        Vertex fruit = g.addV("fruit").property("type", "taxonomy").next();
        g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("a", "b").iterate();
        Vertex s1 = g.V().has("type", "taxonomy").next();
        List<Vertex> s2 = g.V().has("type", "plant").next(2);
        List<Edge> things = g.E().has("a", "b").toList();
        assertEquals(2, (long) g.V(fruit.id()).inE().count().next());

        TraversalMetrics prof = g.V()
                .has("type", "taxonomy").as("a")
                .V().has("type", "plant").as("b")
                .addE("IsA").from("b").to("a").property("a", "b").profile().next();
        Metrics fm = (Metrics) prof.getMetrics().toArray()[4];
        assertEquals(3, fm.getNested("FireflyMetrics").getAnnotations().size());
        System.out.println(prof);
    }

    @Override
    protected boolean clearData() {
        return false;
    }
}
