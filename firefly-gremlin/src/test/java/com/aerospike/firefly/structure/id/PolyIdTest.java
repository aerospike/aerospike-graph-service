package com.aerospike.firefly.structure.id;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.client.util.Crypto;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.impl.relational.RelationalGraph;
import com.aerospike.firefly.io.impl.relational.packed.PackedVertex;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

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

    public void testFireflyIdPoly() {
        PackedVertex va = (PackedVertex) graph.addVertex(T.id, "A");
        PackedVertex vb = (PackedVertex) graph.addVertex(T.id, "B");

        assertEquals("A", va.id.getUserId());
        assertEquals("B", vb.id.getUserId());

        Record reca = va.getBaseElement();
        Record recb = vb.getBaseElement();

        assertEquals("A", reca.getString(AerospikeConnection.USER_KEY));
        assertEquals("B", recb.getString(AerospikeConnection.USER_KEY));

        Record readbackByHash = db.getClient().get(null, new Key(db.getNamespace(), vb.id.getKeyHash(), db.VERTEX_AERO_SET, Value.NULL));
        assertEquals(recb, readbackByHash);

        Record readbackByUser = db.getClient().get(null, new Key(db.getNamespace(), db.VERTEX_AERO_SET, Value.get(vb.id.getUserId())));
        assertEquals(recb, readbackByUser);

        assertArrayEquals(vb.id.getKeyHash(), new Key(db.getNamespace(), db.VERTEX_AERO_SET, Value.get(vb.id.getUserId())).digest);
    }

    @Test
    public void testFireflyIdPolyComposite() {
        PackedVertex va = (PackedVertex) graph.addVertex(T.id, "A");
        PackedVertex vb = (PackedVertex) graph.addVertex(T.id, "B");

        assertEquals("A", va.id.getUserId());
        assertEquals("B", vb.id.getUserId());

        Record reca = va.getBaseElement();
        Record recb = vb.getBaseElement();

        assertEquals("A", reca.getString(AerospikeConnection.USER_KEY));
        assertEquals("B", recb.getString(AerospikeConnection.USER_KEY));

        FireflyEdge eab = (FireflyEdge) va.addEdge("knows", vb);
        FireflyEdge eba = (FireflyEdge) vb.addEdge("forgot", va);

        List<FireflyId> edgeIds = vb.getEdgeIdsFromVertex(Direction.OUT);
        assertEquals(1, edgeIds.size());
        assertEquals(eba.id(), edgeIds.get(0).getUserId());
        FireflyIdComposite fidc = (FireflyIdComposite) edgeIds.get(0);
        FireflyId eidRecovered = fidc.getEdgeId();
        FireflyId aidRecovered = fidc.getAdjacentId();
        assertEquals(aidRecovered.getUserId(), "A");
        assertArrayEquals(eidRecovered.getKeyHash(), eba.id.getKeyHash());
    }

    @Test
    public void testHashRoundTrip() {
        PackedVertex va = (PackedVertex) graph.addVertex(T.id, "A");
        PackedVertex vb = (PackedVertex) graph.addVertex(T.id, "B");

        assertEquals("A", va.id.getUserId());
        assertEquals("B", vb.id.getUserId());

        Record reca = va.getBaseElement();
        Record recb = vb.getBaseElement();

        assertEquals("A", reca.getString(AerospikeConnection.USER_KEY));
        assertEquals("B", recb.getString(AerospikeConnection.USER_KEY));

        FireflyEdge eab = (FireflyEdge) va.addEdge("knows", vb);
        FireflyEdge eba = (FireflyEdge) vb.addEdge("forgot", va);

        Record eaRec = eab.getBaseElement();
        Record ebRec = eba.getBaseElement();
        String inHashA = eaRec.getString(Direction.IN.name());
        String outHashA = eaRec.getString(Direction.OUT.name());
        String inHashB = ebRec.getString(Direction.IN.name());
        String outHashB = ebRec.getString(Direction.OUT.name());

        byte[] origVAKeyBytes = va.id.getKeyHash();
        String origVAKeyHash = Crypto.encodeBase64(origVAKeyBytes);
        byte[] recoveredVAKeyBytes = Crypto.decodeBase64(outHashA.getBytes(), 0, outHashA.getBytes().length);

        assertArrayEquals(origVAKeyBytes,recoveredVAKeyBytes);
        Key inHashAKey = new Key(db.getNamespace(), recoveredVAKeyBytes, db.VERTEX_AERO_SET, Value.NULL);
        Record rawResult = db.getClient().get(null, inHashAKey);


        FireflyIdPoly recoveredFIDAIn = FireflyIdPoly.fromBase64Hash(inHashA, db.VERTEX_AERO_SET);
        FireflyIdPoly recoveredFIDAOut = FireflyIdPoly.fromBase64Hash(outHashA, db.VERTEX_AERO_SET);


        ((RelationalGraph) graph).readVertex(recoveredFIDAIn);
        ((RelationalGraph) graph).readVertex(recoveredFIDAOut);

    }


}
