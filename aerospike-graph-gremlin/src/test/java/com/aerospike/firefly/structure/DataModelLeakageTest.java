package com.aerospike.firefly.structure;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.client.Value;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.structure.FireflyEdge.PROPERTIES_POSITION;
import static com.aerospike.firefly.structure.FireflyEdge.TYPE_HINTS_POSITION;

public class DataModelLeakageTest {
    private static Configuration CONF = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph SETUP_GRAPH;

    @BeforeClass
    static public void beforeAll() {
        SETUP_GRAPH = FireflyGraph.open(CONF);
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
    }

    @AfterClass
    static public void afterAll() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, true);
        SETUP_GRAPH.close();
    }

    @Before
    public void beforeEach() {
        CONF = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    }

    @After
    public void afterEach() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, false);
    }

    @Test
    public void testOverwritingVertexProperty() {
        final GraphTraversalSource g = SETUP_GRAPH.traversal();
        final Object id = g.addV().next().id();
        final Key recordKey = new Key(SETUP_GRAPH.getBaseGraph().getConfig().namespace, SETUP_GRAPH.getBaseGraph().getConfig().vertexAeroSet,
                Value.get(id));
        g.V(id).property("foo", 123, "meta", 456).property("bar", 123).iterate();
        Object vpId = g.V(id).properties("foo").id().next();
        final Long fooKey = SETUP_GRAPH.getBaseGraph().schemaManager.getVertexPropertyRead("foo");

        Record record = SETUP_GRAPH.getBaseGraph().read(recordKey, null);
        var vpBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyDataBin);
        var vpThBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyTHBin);
        var vppBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vpPropertyBin);
        var vppMap = ((Map<Long, Map>) vppBin.get(fooKey)).get(vpId);
        Assert.assertEquals(2, vpBin.size());
        Assert.assertEquals(2, vpThBin.size());
        Assert.assertEquals(2, vppBin.size());
        Assert.assertEquals(1, vppMap.size());

        g.V(id).property(VertexProperty.Cardinality.single, "foo", 321L, "meta", 654).iterate();
        vpId = g.V(id).properties("foo").id().next();
        record = SETUP_GRAPH.getBaseGraph().read(recordKey, null);
        vpBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyDataBin);
        vpThBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyTHBin);
        vppBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vpPropertyBin);
        vppMap = ((Map<Long, Map>) vppBin.get(fooKey)).get(vpId);
        Assert.assertEquals(2, vpBin.size());
        Assert.assertEquals(2, vpThBin.size());
        Assert.assertEquals(2, vppBin.size());
        Assert.assertEquals(1, vppMap.size());

        g.V(id).property(VertexProperty.Cardinality.single, "foo", null, "meta", 654).iterate();
        record = SETUP_GRAPH.getBaseGraph().read(recordKey, null);
        vpBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyDataBin);
        vpThBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyTHBin);
        vppBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vpPropertyBin);
        Assert.assertEquals(1, vpBin.size());
        Assert.assertEquals(1, vpThBin.size());
        Assert.assertEquals(1, vppBin.size());
        Assert.assertFalse(vppBin.containsKey(fooKey));

        g.V(id).property(VertexProperty.Cardinality.single, "bar", null, "meta", 654).iterate();
        record = SETUP_GRAPH.getBaseGraph().read(recordKey, null);
        vpBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyDataBin);
        vpThBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyTHBin);
        vppBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vpPropertyBin);
        Assert.assertEquals(0, vpBin.size());
        Assert.assertEquals(0, vpThBin.size());
        Assert.assertEquals(0, vppBin.size());
    }

    @Test
    public void testRemovingVertexProperty() {
        final GraphTraversalSource g = SETUP_GRAPH.traversal();
        final Object id = g.addV().next().id();
        final Key recordKey = new Key(SETUP_GRAPH.getBaseGraph().getConfig().namespace, SETUP_GRAPH.getBaseGraph().getConfig().vertexAeroSet,
                Value.get(id));
        g.V(id).property("foo", 123, "meta", 456).property("bar", 123).iterate();
        Object vpId = g.V(id).properties("foo").id().next();
        final Long fooKey = SETUP_GRAPH.getBaseGraph().schemaManager.getVertexPropertyRead("foo");

        Record record = SETUP_GRAPH.getBaseGraph().read(recordKey, null);
        var vpBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyDataBin);
        var vpThBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyTHBin);
        var vppBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vpPropertyBin);
        var vppMap = ((Map<Long, Map>) vppBin.get(fooKey)).get(vpId);
        Assert.assertEquals(2, vpBin.size());
        Assert.assertEquals(2, vpThBin.size());
        Assert.assertEquals(2, vppBin.size());
        Assert.assertEquals(1, vppMap.size());

        g.V(id).properties("foo").drop().iterate();
        record = SETUP_GRAPH.getBaseGraph().read(recordKey, null);
        vpBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyDataBin);
        vpThBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyTHBin);
        vppBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vpPropertyBin);
        Assert.assertEquals(1, vpBin.size());
        Assert.assertEquals(1, vpThBin.size());
        Assert.assertEquals(1, vppBin.size());
        Assert.assertFalse(vppBin.containsKey(fooKey));

        g.V(id).properties("bar").drop().iterate();
        record = SETUP_GRAPH.getBaseGraph().read(recordKey, null);
        vpBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyDataBin);
        vpThBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyTHBin);
        vppBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vpPropertyBin);
        Assert.assertEquals(0, vpBin.size());
        Assert.assertEquals(0, vpThBin.size());
        Assert.assertEquals(0, vppBin.size());
    }

    @Test
    public void testOverwritingVpProperty() {
        final GraphTraversalSource g = SETUP_GRAPH.traversal();
        final Object id = g.addV().next().id();
        final Key recordKey = new Key(SETUP_GRAPH.getBaseGraph().getConfig().namespace, SETUP_GRAPH.getBaseGraph().getConfig().vertexAeroSet,
                Value.get(id));
        g.V(id).property("foo", 123, "bar", 456, "baz", 789).iterate();
        Object vpId = g.V(id).properties("foo").id().next();
        final Long fooKey = SETUP_GRAPH.getBaseGraph().schemaManager.getVertexPropertyRead("foo");
        final Long barKey = SETUP_GRAPH.getBaseGraph().schemaManager.getVpPropertyRead("bar");
        final Long bazKey = SETUP_GRAPH.getBaseGraph().schemaManager.getVpPropertyRead("baz");

        Record record = SETUP_GRAPH.getBaseGraph().read(recordKey, null);
        var vpBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyDataBin);
        var vpThBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyTHBin);
        var vppBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vpPropertyBin);
        var vppMap = ((Map<Long, Map>) vppBin.get(fooKey)).get(vpId);
        Assert.assertEquals(1, vpBin.size());
        Assert.assertEquals(1, vpThBin.size());
        Assert.assertEquals(1, vppBin.size());
        Assert.assertEquals(2, vppMap.size());
        Assert.assertTrue(vppMap.containsKey(barKey));
        Assert.assertTrue(vppMap.containsKey(bazKey));

        g.V(id).properties("foo").property("bar", "fourfivesix").property("baz", "seveneightnine").iterate();
        record = SETUP_GRAPH.getBaseGraph().read(recordKey, null);
        vpBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyDataBin);
        vpThBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyTHBin);
        vppBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vpPropertyBin);
        vppMap = ((Map<Long, Map>) vppBin.get(fooKey)).get(vpId);
        Assert.assertEquals(1, vpBin.size());
        Assert.assertEquals(1, vpThBin.size());
        Assert.assertEquals(1, vppBin.size());
        Assert.assertEquals(2, vppMap.size());
        Assert.assertTrue(vppMap.containsKey(barKey));
        Assert.assertTrue(vppMap.containsKey(bazKey));

        g.V(id).properties("foo").property("bar", null).iterate();
        record = SETUP_GRAPH.getBaseGraph().read(recordKey, null);
        vpBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyDataBin);
        vpThBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyTHBin);
        vppBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vpPropertyBin);
        vppMap = ((Map<Long, Map>) vppBin.get(fooKey)).get(vpId);
        Assert.assertEquals(1, vpBin.size());
        Assert.assertEquals(1, vpThBin.size());
        Assert.assertEquals(1, vppBin.size());
        Assert.assertEquals(1, vppMap.size());
        Assert.assertFalse(vppMap.containsKey(barKey));

        g.V(id).properties("foo").property("baz", null).iterate();
        record = SETUP_GRAPH.getBaseGraph().read(recordKey, null);
        vpBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyDataBin);
        vpThBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyTHBin);
        vppBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vpPropertyBin);
        vppMap = ((Map<Long, Map>) vppBin.get(fooKey)).get(vpId);
        Assert.assertEquals(1, vpBin.size());
        Assert.assertEquals(1, vpThBin.size());
        Assert.assertEquals(1, vppBin.size());
        Assert.assertEquals(0, vppMap.size());
    }

    @Test
    public void testRemovingVpProperty() {
        final GraphTraversalSource g = SETUP_GRAPH.traversal();
        final Object id = g.addV().next().id();
        final Key recordKey = new Key(SETUP_GRAPH.getBaseGraph().getConfig().namespace, SETUP_GRAPH.getBaseGraph().getConfig().vertexAeroSet,
                Value.get(id));
        g.V(id).property("foo", 123, "bar", 456, "baz", 789).iterate();
        Object vpId = g.V(id).properties("foo").id().next();
        final Long fooKey = SETUP_GRAPH.getBaseGraph().schemaManager.getVertexPropertyRead("foo");
        final Long barKey = SETUP_GRAPH.getBaseGraph().schemaManager.getVpPropertyRead("bar");
        final Long bazKey = SETUP_GRAPH.getBaseGraph().schemaManager.getVpPropertyRead("baz");

        Record record = SETUP_GRAPH.getBaseGraph().read(recordKey, null);
        var vpBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyDataBin);
        var vpThBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyTHBin);
        var vppBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vpPropertyBin);
        var vppMap = ((Map<Long, Map>) vppBin.get(fooKey)).get(vpId);
        Assert.assertEquals(1, vpBin.size());
        Assert.assertEquals(1, vpThBin.size());
        Assert.assertEquals(1, vppBin.size());
        Assert.assertEquals(2, vppMap.size());
        Assert.assertTrue(vppMap.containsKey(barKey));
        Assert.assertTrue(vppMap.containsKey(bazKey));

        g.V(id).properties("foo").properties("bar").drop().iterate();
        record = SETUP_GRAPH.getBaseGraph().read(recordKey, null);
        vpBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyDataBin);
        vpThBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyTHBin);
        vppBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vpPropertyBin);
        vppMap = ((Map<Long, Map>) vppBin.get(fooKey)).get(vpId);
        Assert.assertEquals(1, vpBin.size());
        Assert.assertEquals(1, vpThBin.size());
        Assert.assertEquals(1, vppBin.size());
        Assert.assertEquals(1, vppMap.size());
        Assert.assertFalse(vppMap.containsKey(barKey));

        g.V(id).properties("foo").properties("baz").drop().iterate();
        record = SETUP_GRAPH.getBaseGraph().read(recordKey, null);
        vpBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyDataBin);
        vpThBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vertexPropertyTHBin);
        vppBin = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().vpPropertyBin);
        vppMap = ((Map<Long, Map>) vppBin.get(fooKey)).get(vpId);
        Assert.assertEquals(1, vpBin.size());
        Assert.assertEquals(1, vpThBin.size());
        Assert.assertEquals(1, vppBin.size());
        Assert.assertEquals(0, vppMap.size());
    }

    @Test
    public void testRemovingFromEdgeCache() {
        final GraphTraversalSource g = SETUP_GRAPH.traversal();
        final Object fromId = g.addV().next().id();
        final Object toId = g.addV().next().id();
        final Key fromRecordKey = new Key(SETUP_GRAPH.getBaseGraph().getConfig().namespace,
                SETUP_GRAPH.getBaseGraph().getConfig().vertexAeroSet, Value.get(fromId));
        final Key toRecordKey = new Key(SETUP_GRAPH.getBaseGraph().getConfig().namespace,
                SETUP_GRAPH.getBaseGraph().getConfig().vertexAeroSet, Value.get(toId));
        for (int i = 0; i < 2; i++) {
            g.addE("e1").from(__.V(fromId)).to(__.V(toId)).iterate();
            g.addE("e2").from(__.V(fromId)).to(__.V(toId)).iterate();
        }

        Record fromRecord = SETUP_GRAPH.getBaseGraph().read(fromRecordKey, null);
        Record toRecord = SETUP_GRAPH.getBaseGraph().read(toRecordKey, null);
        var outBin = fromRecord.getMap(SETUP_GRAPH.getBaseGraph().getConfig().outEdgesBin);
        var inBin = toRecord.getMap(SETUP_GRAPH.getBaseGraph().getConfig().inEdgesBin);
        Assert.assertEquals(2, outBin.size());
        Assert.assertEquals(2, inBin.size());

        g.E().hasLabel("e1").limit(1).drop().iterate();
        fromRecord = SETUP_GRAPH.getBaseGraph().read(fromRecordKey, null);
        toRecord = SETUP_GRAPH.getBaseGraph().read(toRecordKey, null);
        outBin = fromRecord.getMap(SETUP_GRAPH.getBaseGraph().getConfig().outEdgesBin);
        inBin = toRecord.getMap(SETUP_GRAPH.getBaseGraph().getConfig().inEdgesBin);
        Assert.assertEquals(2, outBin.size());
        Assert.assertEquals(2, inBin.size());

        g.E().hasLabel("e1").limit(1).drop().iterate();
        fromRecord = SETUP_GRAPH.getBaseGraph().read(fromRecordKey, null);
        toRecord = SETUP_GRAPH.getBaseGraph().read(toRecordKey, null);
        outBin = fromRecord.getMap(SETUP_GRAPH.getBaseGraph().getConfig().outEdgesBin);
        inBin = toRecord.getMap(SETUP_GRAPH.getBaseGraph().getConfig().inEdgesBin);
        Assert.assertEquals(1, outBin.size());
        Assert.assertEquals(1, inBin.size());

        g.E().hasLabel("e2").limit(1).drop().iterate();
        fromRecord = SETUP_GRAPH.getBaseGraph().read(fromRecordKey, null);
        toRecord = SETUP_GRAPH.getBaseGraph().read(toRecordKey, null);
        outBin = fromRecord.getMap(SETUP_GRAPH.getBaseGraph().getConfig().outEdgesBin);
        inBin = toRecord.getMap(SETUP_GRAPH.getBaseGraph().getConfig().inEdgesBin);
        Assert.assertEquals(1, outBin.size());
        Assert.assertEquals(1, inBin.size());

        g.E().hasLabel("e2").limit(1).drop().iterate();
        fromRecord = SETUP_GRAPH.getBaseGraph().read(fromRecordKey, null);
        toRecord = SETUP_GRAPH.getBaseGraph().read(toRecordKey, null);
        outBin = fromRecord.getMap(SETUP_GRAPH.getBaseGraph().getConfig().outEdgesBin);
        inBin = toRecord.getMap(SETUP_GRAPH.getBaseGraph().getConfig().inEdgesBin);
        Assert.assertEquals(0, outBin.size());
        Assert.assertEquals(0, inBin.size());
    }

    @Test
    public void testOverwritingEdgeProperty() {
        final GraphTraversalSource g = SETUP_GRAPH.traversal();
        final Object fromId = g.addV().next().id();
        final Object toId = g.addV().next().id();
        final Object edgeId = g.addE("edge").property("foo", 123).property("bar", 456)
                .from(__.V(fromId)).to(__.V(toId)).id().next();
        final Object recordId = SETUP_GRAPH.getBaseGraph().getIdFactory().createEdgeId(edgeId).getStorageId();
        final Key key = new Key(SETUP_GRAPH.getBaseGraph().getConfig().namespace,
                SETUP_GRAPH.getBaseGraph().getConfig().edgeAeroSet, Value.get(recordId));

        Record record = SETUP_GRAPH.getBaseGraph().read(key, null);
        var phatEdgeDataMap = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().edgeDataBin);
        List<Object> edgeDataList = (List<Object>) phatEdgeDataMap.values().iterator().next();
        Map<Long, Object> edgePropertiesMap = (Map<Long, Object>) edgeDataList.get(PROPERTIES_POSITION);
        Map<Long, Long> edgeThMap = (Map<Long, Long>) edgeDataList.get(TYPE_HINTS_POSITION);
        Assert.assertEquals(1, phatEdgeDataMap.size());
        Assert.assertEquals(2, edgePropertiesMap.size());
        Assert.assertEquals(2, edgeThMap.size());

        g.E(edgeId).property("foo", "onetwothree").property("bar", 123).iterate();
        record = SETUP_GRAPH.getBaseGraph().read(key, null);
        phatEdgeDataMap = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().edgeDataBin);
        edgeDataList = (List<Object>) phatEdgeDataMap.values().iterator().next();
        edgePropertiesMap = (Map<Long, Object>) edgeDataList.get(PROPERTIES_POSITION);
        edgeThMap = (Map<Long, Long>) edgeDataList.get(TYPE_HINTS_POSITION);
        Assert.assertEquals(1, phatEdgeDataMap.size());
        Assert.assertEquals(2, edgePropertiesMap.size());
        Assert.assertEquals(1, edgeThMap.size());

        g.E(edgeId).property("foo", null).property("bar", null).iterate();
        record = SETUP_GRAPH.getBaseGraph().read(key, null);
        phatEdgeDataMap = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().edgeDataBin);
        edgeDataList = (List<Object>) phatEdgeDataMap.values().iterator().next();
        edgePropertiesMap = (Map<Long, Object>) edgeDataList.get(PROPERTIES_POSITION);
        edgeThMap = (Map<Long, Long>) edgeDataList.get(TYPE_HINTS_POSITION);
        Assert.assertEquals(1, phatEdgeDataMap.size());
        Assert.assertEquals(0, edgePropertiesMap.size());
        Assert.assertEquals(0, edgeThMap.size());
    }

    @Test
    public void testRemovingEdgeProperty() {
        final GraphTraversalSource g = SETUP_GRAPH.traversal();
        final Object fromId = g.addV().next().id();
        final Object toId = g.addV().next().id();
        final Object edgeId = g.addE("edge").property("foo", 123).property("bar", "fourfivesix")
                .from(__.V(fromId)).to(__.V(toId)).id().next();
        final Object recordId = SETUP_GRAPH.getBaseGraph().getIdFactory().createEdgeId(edgeId).getStorageId();
        final Key key = new Key(SETUP_GRAPH.getBaseGraph().getConfig().namespace,
                SETUP_GRAPH.getBaseGraph().getConfig().edgeAeroSet, Value.get(recordId));

        Record record = SETUP_GRAPH.getBaseGraph().read(key, null);
        var phatEdgeDataMap = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().edgeDataBin);
        List<Object> edgeDataList = (List<Object>) phatEdgeDataMap.values().iterator().next();
        Map<Long, Object> edgePropertiesMap = (Map<Long, Object>) edgeDataList.get(PROPERTIES_POSITION);
        Map<Long, Long> edgeThMap = (Map<Long, Long>) edgeDataList.get(TYPE_HINTS_POSITION);
        Assert.assertEquals(1, phatEdgeDataMap.size());
        Assert.assertEquals(2, edgePropertiesMap.size());
        Assert.assertEquals(1, edgeThMap.size());

        g.E(edgeId).properties("foo").drop().iterate();
        record = SETUP_GRAPH.getBaseGraph().read(key, null);
        phatEdgeDataMap = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().edgeDataBin);
        edgeDataList = (List<Object>) phatEdgeDataMap.values().iterator().next();
        edgePropertiesMap = (Map<Long, Object>) edgeDataList.get(PROPERTIES_POSITION);
        edgeThMap = (Map<Long, Long>) edgeDataList.get(TYPE_HINTS_POSITION);
        Assert.assertEquals(1, phatEdgeDataMap.size());
        Assert.assertEquals(1, edgePropertiesMap.size());
        Assert.assertEquals(0, edgeThMap.size());

        g.E(edgeId).properties("bar").drop().iterate();
        record = SETUP_GRAPH.getBaseGraph().read(key, null);
        phatEdgeDataMap = record.getMap(SETUP_GRAPH.getBaseGraph().getConfig().edgeDataBin);
        edgeDataList = (List<Object>) phatEdgeDataMap.values().iterator().next();
        edgePropertiesMap = (Map<Long, Object>) edgeDataList.get(PROPERTIES_POSITION);
        edgeThMap = (Map<Long, Long>) edgeDataList.get(TYPE_HINTS_POSITION);
        Assert.assertEquals(1, phatEdgeDataMap.size());
        Assert.assertEquals(0, edgePropertiesMap.size());
        Assert.assertEquals(0, edgeThMap.size());
    }

    @Test
    public void testOverWritingSupernodeEdgeProperty() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, 0);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();
            final Object fromId = g.addV().next().id();
            final Object toId = g.addV().next().id();
            final Object edgeId = g.addE("edge").property("foo", 123).property("bar", 456)
                    .from(__.V(fromId)).to(__.V(toId)).id().next();
            final Object recordId = graph.getBaseGraph().getIdFactory().createEdgeId(edgeId).getStorageId();
            final Key key = new Key(graph.getBaseGraph().getConfig().namespace,
                    graph.getBaseGraph().getConfig().edgeAeroSet, Value.get(recordId));

            Record record = graph.getBaseGraph().read(key, null);
            var phatEdgeDataMap = record.getMap(graph.getBaseGraph().getConfig().edgeDataBin);
            Map<Long, Long> edgeThMap = (Map<Long, Long>) phatEdgeDataMap.values().iterator().next();
            Map<Long, Object> edgePropertiesOutMap = (Map<Long, Object>) (record.getMap(graph.getBaseGraph().getConfig().supernodesOutBin).values().iterator().next());
            Map<Long, Object> edgePropertiesInMap = (Map<Long, Object>) (record.getMap(graph.getBaseGraph().getConfig().supernodesInBin).values().iterator().next());
            Assert.assertEquals(2, edgeThMap.size());
            // These bins contain 2 extra values - label and adjacent vertex id
            Assert.assertEquals(4, edgePropertiesOutMap.size());
            Assert.assertEquals(4, edgePropertiesInMap.size());

            g.E(edgeId).property("foo", "onetwothree").property("bar", 123).iterate();
            record = graph.getBaseGraph().read(key, null);
            phatEdgeDataMap = record.getMap(graph.getBaseGraph().getConfig().edgeDataBin);
            edgeThMap = (Map<Long, Long>) phatEdgeDataMap.values().iterator().next();
            edgePropertiesOutMap = (Map<Long, Object>) (record.getMap(graph.getBaseGraph().getConfig().supernodesOutBin).values().iterator().next());
            edgePropertiesInMap = (Map<Long, Object>) (record.getMap(graph.getBaseGraph().getConfig().supernodesInBin).values().iterator().next());
            Assert.assertEquals(1, edgeThMap.size());
            // These bins contain 2 extra values - label and adjacent vertex id
            Assert.assertEquals(4, edgePropertiesOutMap.size());
            Assert.assertEquals(4, edgePropertiesInMap.size());

            g.E(edgeId).property("foo", null).property("bar", null).iterate();
            record = graph.getBaseGraph().read(key, null);
            phatEdgeDataMap = record.getMap(graph.getBaseGraph().getConfig().edgeDataBin);
            edgeThMap = (Map<Long, Long>) phatEdgeDataMap.values().iterator().next();
            edgePropertiesOutMap = (Map<Long, Object>) (record.getMap(graph.getBaseGraph().getConfig().supernodesOutBin).values().iterator().next());
            edgePropertiesInMap = (Map<Long, Object>) (record.getMap(graph.getBaseGraph().getConfig().supernodesInBin).values().iterator().next());
            Assert.assertEquals(0, edgeThMap.size());
            // These bins contain 2 extra values - label and adjacent vertex id
            Assert.assertEquals(2, edgePropertiesOutMap.size());
            Assert.assertEquals(2, edgePropertiesInMap.size());
        }
    }

    @Test
    public void testRemovingSupernodeEdgeProperty() {
        final Configuration config = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
        config.setProperty(ConfigurationHelper.Keys.ON_RECORD_ID_LIMIT, 0);
        try (final FireflyGraph graph = FireflyGraph.open(config)) {
            final GraphTraversalSource g = graph.traversal();
            final Object fromId = g.addV().next().id();
            final Object toId = g.addV().next().id();
            final Object edgeId = g.addE("edge").property("foo", 123).property("bar", "fourfivesix")
                    .from(__.V(fromId)).to(__.V(toId)).id().next();
            final Object recordId = graph.getBaseGraph().getIdFactory().createEdgeId(edgeId).getStorageId();
            final Key key = new Key(graph.getBaseGraph().getConfig().namespace,
                    graph.getBaseGraph().getConfig().edgeAeroSet, Value.get(recordId));

            Record record = graph.getBaseGraph().read(key, null);
            var phatEdgeDataMap = record.getMap(graph.getBaseGraph().getConfig().edgeDataBin);
            Map<Long, Long> edgeThMap = (Map<Long, Long>) phatEdgeDataMap.values().iterator().next();
            Map<Long, Object> edgePropertiesOutMap = (Map<Long, Object>) (record.getMap(graph.getBaseGraph().getConfig().supernodesOutBin).values().iterator().next());
            Map<Long, Object> edgePropertiesInMap = (Map<Long, Object>) (record.getMap(graph.getBaseGraph().getConfig().supernodesInBin).values().iterator().next());
            Assert.assertEquals(1, edgeThMap.size());
            // These bins contain 2 extra values - label and adjacent vertex id
            Assert.assertEquals(4, edgePropertiesOutMap.size());
            Assert.assertEquals(4, edgePropertiesInMap.size());

            g.E(edgeId).properties("foo").drop().iterate();
            record = graph.getBaseGraph().read(key, null);
            phatEdgeDataMap = record.getMap(graph.getBaseGraph().getConfig().edgeDataBin);
            edgeThMap = (Map<Long, Long>) phatEdgeDataMap.values().iterator().next();
            edgePropertiesOutMap = (Map<Long, Object>) (record.getMap(graph.getBaseGraph().getConfig().supernodesOutBin).values().iterator().next());
            edgePropertiesInMap = (Map<Long, Object>) (record.getMap(graph.getBaseGraph().getConfig().supernodesInBin).values().iterator().next());
            Assert.assertEquals(0, edgeThMap.size());
            // These bins contain 2 extra values - label and adjacent vertex id
            Assert.assertEquals(3, edgePropertiesOutMap.size());
            Assert.assertEquals(3, edgePropertiesInMap.size());

            g.E(edgeId).properties("bar").drop().iterate();
            record = graph.getBaseGraph().read(key, null);
            phatEdgeDataMap = record.getMap(graph.getBaseGraph().getConfig().edgeDataBin);
            edgeThMap = (Map<Long, Long>) phatEdgeDataMap.values().iterator().next();
            edgePropertiesOutMap = (Map<Long, Object>) (record.getMap(graph.getBaseGraph().getConfig().supernodesOutBin).values().iterator().next());
            edgePropertiesInMap = (Map<Long, Object>) (record.getMap(graph.getBaseGraph().getConfig().supernodesInBin).values().iterator().next());
            Assert.assertEquals(0, edgeThMap.size());
            // These bins contain 2 extra values - label and adjacent vertex id
            Assert.assertEquals(2, edgePropertiesOutMap.size());
            Assert.assertEquals(2, edgePropertiesInMap.size());
        }
    }
}
