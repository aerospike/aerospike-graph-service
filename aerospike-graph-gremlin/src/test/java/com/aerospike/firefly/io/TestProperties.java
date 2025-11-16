package com.aerospike.firefly.io;

import com.aerospike.client.Key;
import com.aerospike.client.Record;
import com.aerospike.firefly.io.utils.PropertyInsertionBenchmark;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.exceptions.AerospikeGraphElementNotFoundException;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;
import static com.aerospike.firefly.util.DateTimeUtil.testDateTimePropertiesCases;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class TestProperties {
    @Rule
    public TestName testName = new TestName();
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph SETUP_GRAPH;
    protected FireflyGraph graph;

    @BeforeClass
    static public void beforeAll() {
        CONFIG.setProperty(ConfigurationHelper.Keys.HTTP_ENABLED.toLowerCase(), "false");
        SETUP_GRAPH = FireflyGraph.open(CONFIG);
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, true);
    }

    @AfterClass
    static public void afterAll() {
        SETUP_GRAPH.getBaseGraph().dropDatabase(SETUP_GRAPH, true);
        SETUP_GRAPH.close();
    }

    @Before
    public void beforeEach() {
        System.out.println("===> Running " + testName.getMethodName() + " <===");
        graph = FireflyGraph.open(CONFIG);
        final GraphTraversalSource g = graph.traversal();
        Vertex person = g.addV("person").next();
        Vertex car = g.addV("vehicle").next();
        g.addE("bought")
                .property("year", "2022")
                .property("month", "dec")
                .from(person).to(car).iterate();
        g.addE("owns")
                .property("year", "2023")
                .property("month", "jan")
                .from(person).to(car).iterate();
    }

    @After
    public void afterEach() {
        System.out.println("===> Completed " + testName.getMethodName() + " <===");
        graph.getBaseGraph().dropDatabase(graph, false);
        graph.close();
    }

    @Test
    public void testPropertiesWeight() {
        final GraphTraversalSource g = graph.traversal();
        g.V().drop().iterate();

        g.addV("person").property("name", "Lyndon").iterate();
        g.addV("person").property("name", "Simon").iterate();
        g.addE("knows").property("weight", 0.5).
                from(__.V().has("name", "Lyndon")).
                to(__.V().has("name", "Simon")).iterate();
        List<Vertex> v = g.V().has("name", "Lyndon").toList();
        List<Edge> e = g.V().has("name", "Lyndon").outE("knows").toList();
        Traversal<Vertex, Vertex> traversal = g.V().
                has("name", "Lyndon").
                property("weight",
                        __.outE("knows").
                                values("weight").sum(),
                        "acl", "private");

        Vertex lyndon = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals("person", lyndon.label());
        Assert.assertEquals("Lyndon", lyndon.value("name"));
        Assert.assertEquals(0.5, lyndon.value("weight"), 0.01);
        Assert.assertEquals("private", lyndon.property("weight").value("acl"));
        Assert.assertEquals(2L, IteratorUtils.count(lyndon.properties()));
        Assert.assertEquals(1L, IteratorUtils.count(lyndon.property("weight").properties()));
    }

    @Test
    public void testEdgeProperties() {
        final GraphTraversalSource g = graph.traversal();

        // Test adding (done in beforeEach) and filtering.
        var edgeTraversal = g.E().has("year", "2022");
        Edge bought = edgeTraversal.next();
        Assert.assertEquals(2, (long) g.E().count().next());
        Assert.assertEquals("bought", bought.label());
        Assert.assertFalse(edgeTraversal.hasNext());
        HashSet<String> expected = new HashSet<>() {{
            add("year");
            add("month");
        }};
        var properties = g.E().hasLabel("bought").properties();
        while (properties.hasNext()) {
            final Property property = properties.next();
            expected.remove(property.key());
        }
        Assert.assertTrue(expected.isEmpty());
        edgeTraversal = g.E().has("year", "2023");
        Edge owns = edgeTraversal.next();
        Assert.assertEquals("owns", owns.label());
        Assert.assertFalse(edgeTraversal.hasNext());

        // Test removal
        g.E().hasLabel("bought").properties("month").drop().iterate();
        edgeTraversal = g.E().has("year", "2022");
        bought = edgeTraversal.next();
        Assert.assertEquals("bought", bought.label());
        Assert.assertFalse(edgeTraversal.hasNext());
        edgeTraversal = g.E().has("month", "dec");
        Assert.assertFalse(edgeTraversal.hasNext());
        properties = g.E().hasLabel("bought").properties();
        Property year = properties.next();
        Assert.assertFalse(properties.hasNext());
        Assert.assertEquals("year", year.key());
        Assert.assertEquals("2022", year.value());
    }

    @Test
    public void testVertexPropertyProperties() {
        final GraphTraversalSource g = graph.traversal();

        // Test adding and filtering
        g.V().hasLabel("person").property("name", "simon").property("age", "trente").iterate();
        g.V().hasLabel("person").properties("name")
                .property("language", "english")
                .property("length", "five")
                .iterate();
        g.V().hasLabel("person").properties("age")
                .property("language", "french")
                .property("length", "six")
                .iterate();
        var englishVps = g.V().properties().has("language", "english");
        var name = englishVps.next();
        Assert.assertEquals("name", name.key());
        Assert.assertEquals("simon", name.value());
        Assert.assertFalse(englishVps.hasNext());
        var lengthFive = g.V().properties().has("length", "five");
        name = lengthFive.next();
        Assert.assertEquals("name", name.key());
        Assert.assertEquals("simon", name.value());
        Assert.assertFalse(lengthFive.hasNext());
        var vertexProperties = g.V().properties().has("language");
        var vertexProperty = vertexProperties.next();
        Assert.assertTrue((vertexProperty.key().equals("name") && vertexProperty.value().equals("simon")) ||
                (vertexProperty.key().equals("age") && vertexProperty.value().equals("trente")));
        vertexProperty = vertexProperties.next();
        Assert.assertTrue((vertexProperty.key().equals("name") && vertexProperty.value().equals("simon")) ||
                (vertexProperty.key().equals("age") && vertexProperty.value().equals("trente")));
        Assert.assertFalse(vertexProperties.hasNext());

        // Test removal
        g.V().properties().has("language", "english").properties("language").drop().iterate();
        var vpsWithALanguage = g.V().properties().has("language");
        var age = vpsWithALanguage.next();
        Assert.assertEquals("age", age.key());
        Assert.assertEquals("trente", age.value());
        Assert.assertFalse(vpsWithALanguage.hasNext());
        var vpsWithALength = g.V().properties().has("length");
        vertexProperty = vpsWithALength.next();
        Assert.assertTrue((vertexProperty.key().equals("name") && vertexProperty.value().equals("simon")) ||
                (vertexProperty.key().equals("age") && vertexProperty.value().equals("trente")));
        vertexProperty = vpsWithALength.next();
        Assert.assertTrue((vertexProperty.key().equals("name") && vertexProperty.value().equals("simon")) ||
                (vertexProperty.key().equals("age") && vertexProperty.value().equals("trente")));
        Assert.assertFalse(vpsWithALength.hasNext());

        // Test removal of all properties
        g.V().properties().has("length", "five").properties("length").drop().iterate();
        var vpWithLengthFive = g.V().properties().has("length", "five");
        Assert.assertFalse(vpWithLengthFive.hasNext());
        // Ensure the VP still exists
        var names = g.V().hasLabel("person").properties("name");
        name = names.next();
        Assert.assertEquals("simon", name.value());
        Assert.assertFalse(names.hasNext());

        // Test adding a property to a now empty vertex property"s property map
        g.V().hasLabel("person").properties("name").property("length", 5).iterate();
        var vpsWithLength5 = g.V().properties().has("length", 5);
        name = vpsWithLength5.next();
        Assert.assertEquals("name", name.key());
        Assert.assertEquals("simon", name.value());
        Assert.assertFalse(vpsWithLength5.hasNext());

        // Test type hint for a shared key of the VP and its property
        g.V().hasLabel("person").properties("age").property("age", 1).iterate();
        var ageVps = g.V().hasLabel("person").properties("age");
        var ageVp = ageVps.next();
        Assert.assertEquals("age", ageVp.key());
        Assert.assertEquals("trente", ageVp.value());
        Assert.assertFalse(ageVps.hasNext());
        ageVps = g.V().hasLabel("person").properties("age").property("age", 1);
        ageVp = ageVps.next();
        Assert.assertEquals("age", ageVp.key());
        Assert.assertEquals("trente", ageVp.value());
        Assert.assertFalse(ageVps.hasNext());
    }

    @Test
    public void testResiliency() throws InterruptedException {
        final GraphTraversalSource g = graph.traversal();
        g.V().drop().iterate();
        g.addV("person").iterate();
        System.out.println("Sleep 15 seconds");
        Thread.sleep(1000 * 15);
        System.out.println("Wake up");
        List<Vertex> vs = g.V().hasLabel("person").toList();
        Assert.assertEquals(1, vs.size());
    }

    @Test
    public void testDuplicateVertexPropertyProperties() {
        final GraphTraversalSource g = graph.traversal();

        Assert.assertEquals(2, (long) g.V().count().next());

        // Check that multiple properties can be added properly to a vertex property.
        g.V().hasLabel("person").property("name", "simon", "since", 1994, "language", "english").iterate();
        var nameTraversal = g.V().properties().has("since", 1994).has("language", "english");
        var name = nameTraversal.next();
        Assert.assertEquals("simon", name.value());
        Assert.assertFalse(nameTraversal.hasNext());
        var namePropertiesTraversal = g.V().properties("name").properties();
        int expectedCount = 2;
        boolean seenSince = false;
        boolean seenLanguage = false;
        while (namePropertiesTraversal.hasNext()) {
            expectedCount--;
            var nameProperty = namePropertiesTraversal.next();
            if (nameProperty.key().equals("since")) {
                Assert.assertFalse(seenSince);
                seenSince = true;
                Assert.assertEquals(1994, nameProperty.value());
            } else if (nameProperty.key().equals("language")) {
                Assert.assertFalse(seenLanguage);
                seenLanguage = true;
                Assert.assertEquals("english", nameProperty.value());
            } else {
                Assert.fail();
            }
        }
        Assert.assertEquals(0, expectedCount);
        Assert.assertTrue(seenSince);
        Assert.assertTrue(seenLanguage);

        // Check that type hints are updated.
        g.V().properties("name").property("since", "1994").iterate();
        namePropertiesTraversal = g.V().properties("name").properties();
        expectedCount = 2;
        seenSince = false;
        seenLanguage = false;
        while (namePropertiesTraversal.hasNext()) {
            expectedCount--;
            var nameProperty = namePropertiesTraversal.next();
            if (nameProperty.key().equals("since")) {
                Assert.assertFalse(seenSince);
                seenSince = true;
                Assert.assertEquals("1994", nameProperty.value());
            } else if (nameProperty.key().equals("language")) {
                Assert.assertFalse(seenLanguage);
                seenLanguage = true;
                Assert.assertEquals("english", nameProperty.value());
            } else {
                Assert.fail();
            }
        }
        Assert.assertEquals(0, expectedCount);
        Assert.assertTrue(seenSince);
        Assert.assertTrue(seenLanguage);

        // Check multiple vertex properties with the same properties on the same vertex.
        g.V().hasLabel("person").property("age", 500, "since", 1500, "language", "english").iterate();
        nameTraversal = g.V().properties().has("since", "1994").has("language", "english");
        name = nameTraversal.next();
        Assert.assertEquals("simon", name.value());
        Assert.assertFalse(nameTraversal.hasNext());
        var ageTraversal = g.V().properties().has("since", 1500).has("language", "english");
        var age = ageTraversal.next();
        Assert.assertEquals(500, age.value());
        Assert.assertFalse(ageTraversal.hasNext());
        g.V().hasLabel("person").properties("age").properties("language").drop().iterate();
        Assert.assertFalse(g.V().properties("age").has("language", "english").hasNext());
        Assert.assertTrue(g.V().properties("name").has("language", "english").hasNext());
        g.V().hasLabel("person").properties("age").drop().iterate();
        nameTraversal = g.V().properties().has("since", "1994").has("language", "english");
        name = nameTraversal.next();
        Assert.assertEquals("simon", name.value());
        Assert.assertFalse(nameTraversal.hasNext());

        // Assert that vertex property properties did not leak into a different record.
        g.V().hasLabel("person").drop().iterate();
        Assert.assertEquals(1, (long) g.V().count().next());
    }

    @Test
    public void testNullEdgeProperties() {
        final GraphTraversalSource g = graph.traversal();

        // "bought" edge
        GraphTraversal traversal = g.V().outE("bought").properties().count();
        long propertiesCount = (long) traversal.next();
        Assert.assertEquals(2, propertiesCount);
        Assert.assertFalse(traversal.hasNext());
        traversal = g.V().outE("bought").has("year");
        Edge edge = (Edge) traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Property property = edge.property("year");
        Assert.assertEquals("2022", property.value());

        // Write null value
        g.V().outE("bought").property("year", null).iterate();
        traversal = g.V().outE("bought").properties().count();
        propertiesCount = (long) traversal.next();
        Assert.assertEquals(1, propertiesCount);
        Assert.assertFalse(traversal.hasNext());
        Assert.assertFalse(g.V().outE("bought").has("year").hasNext());
        Assert.assertFalse(g.V().outE("bought").has("year", (Object) null).hasNext());
        g.V().outE("bought").property("notExistingKey", null).iterate();
        Assert.assertFalse(g.V().outE("bought").has("notExistingKey").hasNext());

        // Test null in a list
        final List<Long> ownedYears = new ArrayList<>();
        ownedYears.add(2022L);
        ownedYears.add(null);
        ownedYears.add(2023L);
        g.V().outE("bought").property("year", ownedYears).iterate();
        traversal = g.V().outE("bought").properties().count();
        propertiesCount = (long) traversal.next();
        Assert.assertEquals(2, propertiesCount);
        Assert.assertFalse(traversal.hasNext());
        traversal = g.V().outE("bought").has("year", new LinkedList<>(ownedYears));
        edge = (Edge) traversal.next();
        Assert.assertFalse(traversal.hasNext());
        property = edge.property("year");
        assertCollectionEquals(new ArrayList<>(ownedYears), (List<Object>) property.value());
    }

    @Test
    public void testNullVertexProperties() {
        final GraphTraversalSource g = graph.traversal();

        // "person" vertex
        g.V().hasLabel("person").property("name", "Simon").property("age", 12).iterate();
        GraphTraversal traversal = g.V().hasLabel("person").properties().count();
        long propertiesCount = (long) traversal.next();
        Assert.assertEquals(2, propertiesCount);
        Assert.assertFalse(traversal.hasNext());
        traversal = g.V().hasLabel("person").has("age");
        Vertex vertex = (Vertex) traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Property property = vertex.property("age");
        Assert.assertEquals(12, property.value());

        // Write null value
        g.V().hasLabel("person").property(VertexProperty.Cardinality.single, "age", null).iterate();
        traversal = g.V().hasLabel("person").properties().count();
        propertiesCount = (long) traversal.next();
        // TODO GRAPH-301: Null does not remove the property in Linked model since cardinality is not Single.
        //                 GRAPH-301 introduces support for multi-properties in Packed so revisit this.

        Assert.assertEquals(1, propertiesCount);
        Assert.assertFalse(traversal.hasNext());
        Assert.assertFalse(g.V().hasLabel("person").has("age").hasNext());
        Assert.assertFalse(g.V().hasLabel("person").has("age", (Object) null).hasNext());
        g.V().hasLabel("person").property(VertexProperty.Cardinality.single, "notExistingKey", null).iterate();
        Assert.assertFalse(g.V().hasLabel("person").has("notExistingKey").hasNext());

        final Set<String> names = new HashSet<>();
        names.add("simon");
        names.add("bauto");
        g.V().hasLabel("person").properties().drop().iterate();
        g.V().hasLabel("person").property("age", 12).property(VertexProperty.Cardinality.list, "name", "simon").property(VertexProperty.Cardinality.list, "name", "bauto").iterate();
        traversal = g.V().hasLabel("person").properties().count();
        propertiesCount = (long) traversal.next();
        Assert.assertEquals(3, propertiesCount);
        Assert.assertFalse(traversal.hasNext());
        traversal = g.V().hasLabel("person").has("name", "simon").has("name", "bauto");
        vertex = (Vertex) traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Iterator<VertexProperty<Object>> properties = vertex.properties("name");
        // compare properties to names
        Set<Object> values = new HashSet<>();
        while (properties.hasNext()) {
            values.add(properties.next().value());
        }
        Assert.assertEquals(names, values);
    }

    @Test
    public void testNullVertexPropertyProperties() {
        final GraphTraversalSource g = graph.traversal();

        // "name" vertex property
        g.V().hasLabel("person").property("name", "Simon").properties("name").property("language", "english").property("addedYear", 2000).iterate();
        GraphTraversal traversal = g.V().hasLabel("person").properties("name").properties().count();
        long propertiesCount = (long) traversal.next();
        Assert.assertEquals(2, propertiesCount);
        Assert.assertFalse(traversal.hasNext());
        traversal = g.V().hasLabel("person").properties("name").has("language");
        VertexProperty vp = (VertexProperty) traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Property property = vp.property("language");
        Assert.assertEquals("english", property.value());

        // Write null value
        g.V().hasLabel("person").properties("name").property("language", null).iterate();
        traversal = g.V().hasLabel("person").properties("name").properties().count();
        propertiesCount = (long) traversal.next();
        Assert.assertEquals(1, propertiesCount);
        Assert.assertFalse(traversal.hasNext());
        Assert.assertFalse(g.V().hasLabel("person").properties("name").has("language").hasNext());
        Assert.assertFalse(g.V().hasLabel("person").properties("name").has("language", (Object) null).hasNext());
        g.V().hasLabel("person").properties("name").property("notExistingKey", null).iterate();
        Assert.assertFalse(g.V().hasLabel("person").properties("name").has("notExistingKey").hasNext());

        // Test null in a list
        final List<String> languages = new ArrayList<>();
        languages.add("english");
        languages.add(null);
        languages.add("french");
        g.V().hasLabel("person").properties("name").property("language", languages).iterate();
        traversal = g.V().hasLabel("person").properties("name").properties().count();
        propertiesCount = (long) traversal.next();
        Assert.assertEquals(2, propertiesCount);
        Assert.assertFalse(traversal.hasNext());
        traversal = g.V().hasLabel("person").properties("name").has("language", new LinkedList<>(languages));
        vp = (VertexProperty) traversal.next();
        Assert.assertFalse(traversal.hasNext());
        property = vp.property("language");
        assertCollectionEquals(new ArrayList<>(languages), (List<Object>) property.value());
    }

    @Test
    public void testTypeHintsPersistence() {
        final GraphTraversalSource g = graph.traversal();
        final Vertex v1 = g.addV().next();
        final Vertex v2 = g.addV().property("vp", "foo").next();
        final Edge e = g.addE("edge").from(v1).to(v2).next();

        Object propertyValue = Integer.valueOf(123);
        g.V(v1.id()).property("test", propertyValue).iterate();
        g.E(e.id()).property("test", propertyValue).iterate();
        g.V(v2.id()).properties("vp").property("test", propertyValue).iterate();

        Object vertexPValue = g.V(v1.id()).values("test").next();
        Object edgePValue = g.E(e.id()).values("test").next();
        Object vpPValue = g.V(v2.id()).properties("vp").values("test").next();
        Assert.assertEquals(propertyValue, vertexPValue);
        Assert.assertEquals(propertyValue, edgePValue);
        Assert.assertEquals(propertyValue, vpPValue);

        propertyValue = Long.valueOf(456);
        g.V(v1.id()).property("test", propertyValue).iterate();
        g.E(e.id()).property("test", propertyValue).iterate();
        g.V(v2.id()).properties("vp").property("test", propertyValue).iterate();

        vertexPValue = g.V(v1.id()).values("test").next();
        edgePValue = g.E(e.id()).values("test").next();
        vpPValue = g.V(v2.id()).properties("vp").values("test").next();
        Assert.assertEquals(propertyValue, vertexPValue);
        Assert.assertEquals(propertyValue, edgePValue);
        Assert.assertEquals(propertyValue, vpPValue);

        propertyValue = Integer.valueOf(123);
        g.V(v1.id()).property(VertexProperty.Cardinality.single, "test", propertyValue).iterate();
        g.E(e.id()).property("test", propertyValue).iterate();
        g.V(v2.id()).properties("vp").property("test", propertyValue).iterate();

        vertexPValue = g.V(v1.id()).values("test").next();
        edgePValue = g.E(e.id()).values("test").next();
        vpPValue = g.V(v2.id()).properties("vp").values("test").next();
        Assert.assertEquals(propertyValue, vertexPValue);
        Assert.assertEquals(propertyValue, edgePValue);
        Assert.assertEquals(propertyValue, vpPValue);

        propertyValue = "123";
        g.V(v1.id()).property(VertexProperty.Cardinality.single, "test", propertyValue).iterate();
        g.E(e.id()).property("test", propertyValue).iterate();
        g.V(v2.id()).properties("vp").property("test", propertyValue).iterate();

        vertexPValue = g.V(v1.id()).values("test").next();
        edgePValue = g.E(e.id()).values("test").next();
        vpPValue = g.V(v2.id()).properties("vp").values("test").next();
        Assert.assertEquals(propertyValue, vertexPValue);
        Assert.assertEquals(propertyValue, edgePValue);
        Assert.assertEquals(propertyValue, vpPValue);
    }

    @Test
    public void testVpRemovalDataModel() {
        final GraphTraversalSource g = graph.traversal();
        final Key vertexRecordKey = new Key(graph.getBaseGraph().getConfig().namespace, graph.getBaseGraph().getConfig().vertexAeroSet, 123);
        Record vertexRecord;
        Map<Long, Map<Object, List<?>>> vpData;
        Map<Long, Map<Long, Long>> vpTypeHint;
        Map<Long, Map<Long, Map<Long, List<?>>>> vpProperties;
        createDataModelTestVertex(g);
        final Long key1 = graph.getBaseGraph().schemaManager.getVertexPropertyRead("key1");
        final Long key2 = graph.getBaseGraph().schemaManager.getVertexPropertyRead("key2");

        // Remove an unique property key and value
        g.V().hasLabel("test").properties("key2").drop().iterate();
        vertexRecord = graph.getBaseGraph().read(vertexRecordKey, null);
        vpData = (Map<Long, Map<Object, List<?>>>) vertexRecord.getMap(graph.getBaseGraph().getConfig().vertexPropertyDataBin);
        vpTypeHint = (Map<Long, Map<Long, Long>>) vertexRecord.getMap(graph.getBaseGraph().getConfig().vertexPropertyTHBin);
        vpProperties = (Map<Long, Map<Long, Map<Long, List<?>>>>) vertexRecord.getMap(graph.getBaseGraph().getConfig().vpPropertyBin);
        Assert.assertEquals(1, vpData.size());
        Assert.assertEquals(1, vpTypeHint.size());
        Assert.assertEquals(1, vpProperties.size());
        Assert.assertTrue(vpData.containsKey(key1));
        Assert.assertTrue(vpTypeHint.containsKey(key1));
        Assert.assertTrue(vpProperties.containsKey(key1));
        Assert.assertEquals(2, vpData.get(key1).size());
        Assert.assertEquals(3, vpTypeHint.get(key1).size());
        Assert.assertEquals(3, vpProperties.get(key1).size());

        createDataModelTestVertex(g);

        // Remove a shared property key with unique value
        var t = g.V().hasLabel("test").properties("key1");
        while (t.hasNext()) {
            final Property<Object> vp = t.next();
            if ((int) vp.value() == 2) {
                vp.remove();
            }
        }
        vertexRecord = graph.getBaseGraph().read(vertexRecordKey, null);
        vpData = (Map<Long, Map<Object, List<?>>>) vertexRecord.getMap(graph.getBaseGraph().getConfig().vertexPropertyDataBin);
        vpTypeHint = (Map<Long, Map<Long, Long>>) vertexRecord.getMap(graph.getBaseGraph().getConfig().vertexPropertyTHBin);
        vpProperties = (Map<Long, Map<Long, Map<Long, List<?>>>>) vertexRecord.getMap(graph.getBaseGraph().getConfig().vpPropertyBin);
        Assert.assertEquals(2, vpData.size());
        Assert.assertEquals(2, vpTypeHint.size());
        Assert.assertEquals(2, vpProperties.size());
        Assert.assertTrue(vpData.containsKey(key1));
        Assert.assertTrue(vpData.containsKey(key2));
        Assert.assertTrue(vpTypeHint.containsKey(key1));
        Assert.assertTrue(vpTypeHint.containsKey(key2));
        Assert.assertTrue(vpProperties.containsKey(key1));
        Assert.assertTrue(vpProperties.containsKey(key2));
        Assert.assertEquals(1, vpData.get(key1).size());
        Assert.assertEquals(1, vpData.get(key2).size());
        Assert.assertEquals(2, vpTypeHint.get(key1).size());
        Assert.assertEquals(1, vpTypeHint.get(key2).size());
        Assert.assertEquals(2, vpProperties.get(key1).size());
        Assert.assertEquals(1, vpProperties.get(key2).size());

        createDataModelTestVertex(g);

        // Remove a shared property key with shared value
        t = g.V().hasLabel("test").properties("key1");
        while (t.hasNext()) {
            final Property<Object> vp = t.next();
            if ((int) vp.value() == 1) {
                vp.remove();
                break;
            }
        }
        vertexRecord = graph.getBaseGraph().read(vertexRecordKey, null);
        vpData = (Map<Long, Map<Object, List<?>>>) vertexRecord.getMap(graph.getBaseGraph().getConfig().vertexPropertyDataBin);
        vpTypeHint = (Map<Long, Map<Long, Long>>) vertexRecord.getMap(graph.getBaseGraph().getConfig().vertexPropertyTHBin);
        vpProperties = (Map<Long, Map<Long, Map<Long, List<?>>>>) vertexRecord.getMap(graph.getBaseGraph().getConfig().vpPropertyBin);
        Assert.assertEquals(2, vpData.size());
        Assert.assertEquals(2, vpTypeHint.size());
        Assert.assertEquals(2, vpProperties.size());
        Assert.assertTrue(vpData.containsKey(key1));
        Assert.assertTrue(vpData.containsKey(key2));
        Assert.assertTrue(vpTypeHint.containsKey(key1));
        Assert.assertTrue(vpTypeHint.containsKey(key2));
        Assert.assertTrue(vpProperties.containsKey(key1));
        Assert.assertTrue(vpProperties.containsKey(key2));
        Assert.assertEquals(2, vpData.get(key1).size());
        Assert.assertEquals(1, vpData.get(key2).size());
        Assert.assertEquals(2, vpTypeHint.get(key1).size());
        Assert.assertEquals(1, vpTypeHint.get(key2).size());
        Assert.assertEquals(2, vpProperties.get(key1).size());
        Assert.assertEquals(1, vpProperties.get(key2).size());

        // Remove all properties
        t = g.V().hasLabel("test").properties().drop().iterate();
        vertexRecord = graph.getBaseGraph().read(vertexRecordKey, null);
        vpData = (Map<Long, Map<Object, List<?>>>) vertexRecord.getMap(graph.getBaseGraph().getConfig().vertexPropertyDataBin);
        vpTypeHint = (Map<Long, Map<Long, Long>>) vertexRecord.getMap(graph.getBaseGraph().getConfig().vertexPropertyTHBin);
        vpProperties = (Map<Long, Map<Long, Map<Long, List<?>>>>) vertexRecord.getMap(graph.getBaseGraph().getConfig().vpPropertyBin);
        Assert.assertEquals(0, vpData.size());
        Assert.assertEquals(0, vpTypeHint.size());
        Assert.assertEquals(0, vpProperties.size());
    }

    @Test
    public void testMutateRemovedProperty() {
        final GraphTraversalSource g = graph.traversal();
        createDataModelTestVertex(g);

        final Property<Object> key2Val2 = g.V().hasLabel("test").properties("key2").next();
        g.V().hasLabel("test").properties("key2").drop().iterate();
        try {
            ((VertexProperty<Object>) key2Val2).property("new", "test");
            Assert.fail("Mutating a removed VP should throw an exception.");
        } catch (final Exception e) {
            Assert.assertTrue(e instanceof AerospikeGraphElementNotFoundException);
        }
        ((VertexProperty<Object>) key2Val2).property("test").remove();
        key2Val2.remove();
        Assert.assertEquals(3, (long) g.V().hasLabel("test").properties().count().next());

        var t = g.V().hasLabel("test").properties("key1");
        Property<Object> key1Val2 = null;
        while (t.hasNext()) {
            final Property<Object> vp = t.next();
            if ((int) vp.value() == 2) {
                key1Val2 = vp;
                break;
            }
        }
        t = g.V().hasLabel("test").properties("key1");
        while (t.hasNext()) {
            final Property<Object> vp = t.next();
            if ((int) vp.value() == 2) {
                vp.remove();
                break;
            }
        }
        try {
            ((VertexProperty<Object>) key1Val2).property("new", "test");
            Assert.fail("Mutating a removed VP should throw an exception.");
        } catch (final Exception e) {
            Assert.assertTrue(e instanceof AerospikeGraphElementNotFoundException);
        }
        ((VertexProperty<Object>) key1Val2).property("test").remove();
        key1Val2.remove();
        Assert.assertEquals(2, (long) g.V().hasLabel("test").properties().count().next());

        t = g.V().hasLabel("test").properties("key1");
        VertexProperty<Object> key1Val1 = null;
        while (t.hasNext()) {
            final VertexProperty<Object> vp = (VertexProperty<Object>) t.next();
            if ((int) vp.value() == 1) {
                key1Val1 = vp;
                break;
            }
        }
        t = g.V().hasLabel("test").properties("key1");
        while (t.hasNext()) {
            final VertexProperty<Object> vp = (VertexProperty<Object>) t.next();
            if (vp.id().equals(key1Val1.id())) {
                vp.remove();
                break;
            }
        }
        try {
            ((VertexProperty<Object>) key1Val1).property("new", "test");
            Assert.fail("Mutating a removed VP should throw an exception.");
        } catch (final Exception e) {
            Assert.assertTrue(e instanceof AerospikeGraphElementNotFoundException);
        }
        ((VertexProperty<Object>) key1Val1).property("test").remove();
        key1Val1.remove();
        Assert.assertEquals(1, (long) g.V().hasLabel("test").properties().count().next());

        key1Val1 = (VertexProperty<Object>) g.V().hasLabel("test").properties("key1").next();
        g.V().hasLabel("test").drop().iterate();
        try {
            ((VertexProperty<Object>) key1Val1).property("new", "test");
            Assert.fail("Mutating a removed VP should throw an exception.");
        } catch (final Exception e) {
            Assert.assertTrue(e instanceof AerospikeGraphElementNotFoundException);
        }
        ((VertexProperty<Object>) key1Val1).property("test").remove();
        key1Val1.remove();
    }

    static private void createDataModelTestVertex(final GraphTraversalSource g) {
        g.V().drop().iterate();
        g.addV("test").property(T.id, 123).iterate();
        g.V().hasLabel("test").property(VertexProperty.Cardinality.list, "key1", 1, "test", 123).iterate();
        g.V().hasLabel("test").property(VertexProperty.Cardinality.list,"key1", 1, "test", 123).iterate();
        g.V().hasLabel("test").property(VertexProperty.Cardinality.list,"key1", 2, "test", 123).iterate();
        g.V().hasLabel("test").property(VertexProperty.Cardinality.list,"key2", 2, "test", 123).iterate();
    }

    @Test
    public void testDateTimeProperties() {
        final GraphTraversalSource g = graph.traversal();
        testDateTimePropertiesCases(g);
    }

    @Test
    public void testDateTimeMultiSharedWithNumeric() {
        final GraphTraversalSource g = graph.traversal();
        g.V().hasLabel("person").property(VertexProperty.Cardinality.list, "birthday", 10)
                .property(VertexProperty.Cardinality.list, "birthday", new Date(1993, 3, 30)).next();
        final GraphTraversal matchesLongPushdownButShouldReturn = g.V().hasLabel("person").has("birthday", P.lt(20));
        final GraphTraversal matchesLongPushdownButShouldNotReturn = g.V().hasLabel("person").has("birthday", P.lt(new Date(1990, 1, 1)));
        final GraphTraversal matchesDatePushdownButShouldReturn = g.V().hasLabel("person").has("birthday", P.gt(new Date(1990, 1, 1)));
        final GraphTraversal matchesDatePushdownButShouldNotReturn = g.V().hasLabel("person").has("birthday", P.gt(20));
        Assert.assertTrue(matchesLongPushdownButShouldReturn.hasNext());
        Assert.assertFalse(matchesLongPushdownButShouldNotReturn.hasNext());
        Assert.assertTrue(matchesDatePushdownButShouldReturn.hasNext());
        Assert.assertFalse(matchesDatePushdownButShouldNotReturn.hasNext());
    }

    @Test
    public void testSchemaManagerNoMatchedKey() {
        final GraphTraversalSource g = graph.traversal();
        final Long noMatchSchema = graph.getBaseGraph().schemaManager.getVertexLabelRead("thispropertykeywillhaveneverbeenaddedbefore");
        var v1 = g.addV("v1").next();
        for (int i = 0; i < 1000; i++) {
            g.V(v1.id()).property("vpKey" + i, 1L, "vppKey" + i, 1L).next();
            var v2 = g.addV("vLabel" + i).next();
            g.addE("eLabel" + i).from(v1).to(v2).property("epKey" + i, 1L).next();
        }

        try {
            final String vLabel = graph.getBaseGraph().schemaManager.getVertexLabelString(noMatchSchema);
            Assert.fail("Grabbed a valid schema string for vLabel when should have thrown an exception.");
        } catch (final IllegalStateException expected) {
        }
        try {
            final String vp = graph.getBaseGraph().schemaManager.getVertexPropertyString(noMatchSchema);
            Assert.fail("Grabbed a valid schema string for vp when should have thrown an exception.");
        } catch (final IllegalStateException expected) {
        }
        try {
            final String vpp = graph.getBaseGraph().schemaManager.getVpPropertyString(noMatchSchema);
            Assert.fail("Grabbed a valid schema string for vpp when should have thrown an exception.");
        } catch (final IllegalStateException expected) {
        }
        try {
            final String eLabel = graph.getBaseGraph().schemaManager.getEdgeLabelString(noMatchSchema);
            Assert.fail("Grabbed a valid schema string for eLabel when should have thrown an exception.");
        } catch (final IllegalStateException expected) {
        }
        try {
            final String ep = graph.getBaseGraph().schemaManager.getEdgePropertyString(noMatchSchema);
            Assert.fail("Grabbed a valid schema string for ep when should have thrown an exception.");
        } catch (final IllegalStateException expected) {
        }

        Assert.assertFalse(g.V().hasLabel("invalid").hasNext());
        Assert.assertFalse(g.V(v1.id()).out("invalid").hasNext());
        Assert.assertFalse(g.V(v1.id()).outE("invalid").hasNext());
        Assert.assertFalse(g.V(v1.id()).has("invalid").hasNext());
        Assert.assertFalse(g.V(v1.id()).properties("invalid").hasNext());
        Assert.assertFalse(g.V(v1.id()).properties().has("invalid").hasNext());
        Assert.assertFalse(g.V(v1.id()).properties().properties("invalid").hasNext());
        Assert.assertFalse(g.E().hasLabel("invalid").hasNext());
        Assert.assertFalse(g.E().has("invalid").hasNext());
        Assert.assertFalse(g.E().properties("invalid").hasNext());
    }

    @Ignore
    @Test
    public void benchmarkPropertyInsertion() {
        final GraphTraversalSource g = this.graph.traversal();
        final int propertyCount = 5000;
        final int edgePackSize = 5;
        final int reportingGroupSize = 500;
        final PropertyInsertionBenchmark benchmark = new PropertyInsertionBenchmark(g, propertyCount, edgePackSize, reportingGroupSize);
        benchmark.benchmark();
    }

    private static void assertCollectionEquals(final Collection<Object> expected, final Collection<Object> actual) {
        final List<Object> expectedClone = new LinkedList<>(expected);
        for (final Object item : actual) {
            if (!expectedClone.contains(item)) {
                Assert.fail("Expected list did not contain value from actual list.");
            } else {
                expectedClone.remove(item);
            }
        }
        if (!expectedClone.isEmpty()) {
            Assert.fail("Expected list has additional values compared to actual list.");
        }
    }
}
