package com.aerospike.firefly.structure.id;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
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

        final Record fooRecord = db.read(new Key(db.getNamespace(), db.VERTEX_AERO_SET, (Long) foo.id()), null);
        final Map<String, List<Object>> fooOutEdges = (Map<String, List<Object>>) fooRecord.getMap(db.OUT_EDGES_BIN);
        final Map<String, List<Object>> fooInEdges = (Map<String, List<Object>>) fooRecord.getMap(db.IN_EDGES_BIN);

        final Record barRecord = db.read(new Key(db.getNamespace(), db.VERTEX_AERO_SET, (Long) bar.id()), null);
        final Map<String, List<Object>> barOutEdges = (Map<String, List<Object>>) barRecord.getMap(db.OUT_EDGES_BIN);
        final Map<String, List<Object>> barInEdges = (Map<String, List<Object>>) barRecord.getMap(db.IN_EDGES_BIN);

        Assert.assertTrue(fooInEdges.isEmpty());
        Assert.assertTrue(barOutEdges.isEmpty());

        final FireflyId fooId = graph.getIdFactory().createVertexId(foo.id());
        final FireflyId barId = graph.getIdFactory().createVertexId(bar.id());
        final FireflyEdgeId bazId = graph.getIdFactory().createEdgeId(baz.id());
        final FireflyId compositeFooId = graph.getIdFactory().createCompositeEdgeId(bazId, fooId);
        final FireflyId compositeBarId = graph.getIdFactory().createCompositeEdgeId(bazId, barId);

        graph.getIdFactory().convertMapToLazyIdsInPlace(barInEdges, graph, LazyEdgeCacheIdTransform.class);
        graph.getIdFactory().convertMapToLazyIdsInPlace(fooOutEdges, graph, LazyEdgeCacheIdTransform.class);
        final Map<String, List<LazyIdTransform>> barInFireflyIdMap = (Map) barInEdges;
        final Map<String, List<LazyIdTransform>> fooOutFireflyIdMap = (Map) fooOutEdges;

        Assert.assertEquals(1, barInFireflyIdMap.size());
        Assert.assertEquals(1, fooOutFireflyIdMap.size());
        Assert.assertTrue(barInFireflyIdMap.containsKey("baz"));
        Assert.assertTrue(fooOutFireflyIdMap.containsKey("baz"));
        Assert.assertEquals(1, barInFireflyIdMap.get("baz").size());
        Assert.assertEquals(1, fooOutFireflyIdMap.get("baz").size());
        Assert.assertEquals(compositeFooId.getCachedId(), barInFireflyIdMap.get("baz").get(0).transform().getCachedId());
        Assert.assertEquals(compositeBarId.getCachedId(), fooOutFireflyIdMap.get("baz").get(0).transform().getCachedId());
    }
}
