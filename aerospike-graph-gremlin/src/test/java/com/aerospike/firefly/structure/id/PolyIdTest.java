package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.AbstractFireflySuite;
import com.aerospike.client.util.Crypto;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertArrayEquals;
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

    @Test
    public void testFireflyIdPolyComposite() {
        FireflyVertex va = (FireflyVertex) graph.addVertex(T.id, "A");
        FireflyVertex vb = (FireflyVertex) graph.addVertex(T.id, "B");

        assertEquals("A", va.id.getUserId());
        assertEquals("B", vb.id.getUserId());

        FireflyEdge eab = (FireflyEdge) va.addEdge("knows", vb);
        FireflyEdge eba = (FireflyEdge) vb.addEdge("forgot", va);

        List<FireflyId> edgeIds = vb.getCachedIds(Direction.OUT, Set.of());
        assertEquals(1, edgeIds.size());
        assertEquals(eba.id(), Crypto.encodeBase64(((ByteBuffer)edgeIds.get(0).getUserId()).array()));
        FireflyIdComposite fidc = (FireflyIdComposite) edgeIds.get(0);
        FireflyId eidRecovered = fidc.getEdgeId();
        FireflyId aidRecovered = fidc.getAdjacentId();
        assertEquals(aidRecovered.getUserId(), "A");
        assertArrayEquals(eidRecovered.getKeyHash(), eba.id.getKeyHash());
    }
}
