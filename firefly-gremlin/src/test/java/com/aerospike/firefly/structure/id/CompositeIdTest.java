package com.aerospike.firefly.structure.id;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.policy.QueryPolicy;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

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

        final FireflyId fooId = FireflyIdFactory.createId(foo.id());
        final FireflyId barId = FireflyIdFactory.createId(bar.id());
        final FireflyId bazId = FireflyIdFactory.createId(baz.id());
        final FireflyId compositeId = FireflyIdFactory.createEdgeId(bazId, barId, fooId);
        final Map<String, List<FireflyId>> barInFireflyIdMap = FireflyIdFactory.convertMapListObjectToFireflyIdMap(barInEdges);
        final Map<String, List<FireflyId>> fooOutFireflyIdMap = FireflyIdFactory.convertMapListObjectToFireflyIdMap(fooOutEdges);

        Assert.assertEquals(1, barInFireflyIdMap.size());
        Assert.assertEquals(1, fooOutFireflyIdMap.size());
        Assert.assertTrue(barInFireflyIdMap.containsKey("baz"));
        Assert.assertTrue(fooOutFireflyIdMap.containsKey("baz"));
        Assert.assertEquals(1, barInFireflyIdMap.get("baz").size());
        Assert.assertEquals(1, fooOutFireflyIdMap.get("baz").size());
        Assert.assertEquals(compositeId, barInFireflyIdMap.get("baz").get(0));
        Assert.assertEquals(compositeId, fooOutFireflyIdMap.get("baz").get(0));
    }

    @Test
    public void testCompositeIds() {
        final GraphTraversalSource g = graph.traversal();
        final Vertex foo = g.addV("foo").next();
        final Vertex bar = g.addV("bar").next();
        final Edge baz = g.addE("baz").from(foo).to(bar).next();
        final Edge baz2 = g.addE("baz2").from(bar).to(foo).next();
        final Object bar2 = g.V(foo.id()).out().next();
        final Object foo2 = g.V(foo.id()).out().out().next();
        System.out.println(bar2);
        System.out.println("bar : " + bar);
        System.out.println("bar2 : " + bar2);
        System.out.println("foo : " + foo);
        System.out.println("foo2 : " + foo2);
        final Object path = g.V(foo.id()).out().path().next();
        final Object path2 = g.V(foo.id()).out().out().path().next();
        System.out.println("path : " + path);
        System.out.println("path2 : " + path2);
    }
}
