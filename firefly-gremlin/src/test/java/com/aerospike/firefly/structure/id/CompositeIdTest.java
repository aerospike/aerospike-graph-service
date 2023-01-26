package com.aerospike.firefly.structure.id;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.firefly.structure.FireflyEdge;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class CompositeIdTest extends AbstractFireflySuite {
    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void testCompositeId() {
        final GraphTraversalSource g = graph.traversal();
        final Vertex foo = g.addV("foo").next();
        final Vertex bar = g.addV("bar").next();
        final Edge baz = g.addE("baz").from(foo).to(bar).next();

        final Record fooRecord = db.getClient().get(new QueryPolicy(), new Key(db.getNamespace(), db.VERTEX_AERO_SET, (Long) foo.id()));
        final Map<String, List<Object>> fooOutEdges = (Map<String, List<Object>>) fooRecord.getMap(db.OUT_EDGES);
        final Map<String, List<Object>> fooInEdges = (Map<String, List<Object>>) fooRecord.getMap(db.IN_EDGES);

        final Record barRecord = db.getClient().get(new QueryPolicy(), new Key(db.getNamespace(), db.VERTEX_AERO_SET, (Long) bar.id()));
        final Map<String, List<Object>> barOutEdges = (Map<String, List<Object>>) barRecord.getMap(db.OUT_EDGES);
        final Map<String, List<Object>> barInEdges = (Map<String, List<Object>>) barRecord.getMap(db.IN_EDGES);

        Assert.assertNull(fooInEdges);
        Assert.assertNull(barOutEdges);

        final FireflyId fooId = graph.getIdFactory().createId(foo.id(), FireflyVertex.class);
        final FireflyId barId = graph.getIdFactory().createId(bar.id(), FireflyVertex.class);
        final FireflyId bazId = graph.getIdFactory().createId(baz.id(), FireflyEdge.class);
        final FireflyId compositeFooId = graph.getIdFactory().createEdgeId(bazId, fooId);
        final FireflyId compositeBarId = graph.getIdFactory().createEdgeId(bazId, barId);
        final Map<String, List<FireflyId>> barInFireflyIdMap = graph.getIdFactory().convertMapListObjectToFireflyIdMap(barInEdges);
        final Map<String, List<FireflyId>> fooOutFireflyIdMap = graph.getIdFactory().convertMapListObjectToFireflyIdMap(fooOutEdges);

        Assert.assertEquals(1, barInFireflyIdMap.size());
        Assert.assertEquals(1, fooOutFireflyIdMap.size());
        Assert.assertTrue(barInFireflyIdMap.containsKey("baz"));
        Assert.assertTrue(fooOutFireflyIdMap.containsKey("baz"));
        Assert.assertEquals(1, barInFireflyIdMap.get("baz").size());
        Assert.assertEquals(1, fooOutFireflyIdMap.get("baz").size());
        Assert.assertArrayEquals((byte[]) compositeFooId.getCachedId(), (byte[]) barInFireflyIdMap.get("baz").get(0).getCachedId());
        Assert.assertArrayEquals((byte[]) compositeBarId.getCachedId(), (byte[]) fooOutFireflyIdMap.get("baz").get(0).getCachedId());
    }
}
