package com.aerospike.firefly.io;

import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.ArrayList;
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
        Vertex a = graph.addVertex(T.id, 1, "it", 2);
        Vertex b = graph.addVertex(T.id, 2, "it", 1);
        Vertex c = graph.addVertex(T.id, 3, "it", 1);
        a.addEdge("e", b);
        a.addEdge("e", c);
        List<MutableMetrics> metrics = new ArrayList<>();
        MutableMetrics rootMetrics = new MutableMetrics("1", "GraphStep");
        metrics.add(rootMetrics);

        MutableMetrics scanMetrics = new MutableMetrics("1.1", "AerospikeScan");
        scanMetrics.setAnnotation(" name", 12);
        scanMetrics.setAnnotation(" age", 3);
        scanMetrics.setAnnotation(" distance", 6);

        scanMetrics.setCount("propertyKey", 12);
        scanMetrics.setDuration(12, TimeUnit.MILLISECONDS);

//        rootMetrics.addNested(scanMetrics);
        metrics.add(scanMetrics);
        DefaultTraversalMetrics dtm = new DefaultTraversalMetrics(100, metrics);
        System.out.println(dtm);
        TraversalMetrics p = graph.traversal().V().has("it",1).profile().next();
        System.out.println(p);

    }


    @Override
    protected boolean clearData() {
        return false;
    }
}
