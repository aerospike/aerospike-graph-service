package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class PolyIdTest extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void testStringId() {
        GraphTraversalSource g = graph.traversal();
        final String V_ID_A = "VA";
        final String V_ID_B = "VB";
        g
                .addV().property(T.id, V_ID_A).property("name", "tom").as("a")
                .addV().property(T.id, V_ID_B).property("name", "jerry").as("b")
                .addE("chases").from("a").to("b").iterate();
        Long c = g.V(V_ID_A).outE("chases").inV().has("name", "jerry").count().next();
        assertEquals(1L, c.longValue());
    }

    @Test
    public void testKeyHashRecovery() {
        Vertex va = graph.addVertex(T.id, "A");
        Vertex vb = graph.addVertex(T.id, "B");
        va.addEdge("chases", vb);
        assertEquals(va.id(), graph.traversal().V(vb).in().toList().get(0).id());
    }
}
