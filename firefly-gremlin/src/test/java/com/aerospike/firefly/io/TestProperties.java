package com.aerospike.firefly.io;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.HashSet;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class TestProperties {
    private static final Configuration CONFIG = ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES);
    private static FireflyGraph FIREFLY;

    @BeforeClass
    static public void beforeAll() {
        FIREFLY = FireflyGraph.open(CONFIG);
        FIREFLY.getBaseGraph().dropDatabase();
    }

    @AfterClass
    static public void afterAll() {
        FIREFLY.getBaseGraph().dropDatabase();
        FIREFLY.close();
    }

    @Before
    public void beforeEach() {
        final GraphTraversalSource g = FIREFLY.traversal();
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
        FIREFLY.getBaseGraph().dropDatabase();
    }

    @Test
    public void testEdgeProperties() {
        final GraphTraversalSource g = FIREFLY.traversal();

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
        final GraphTraversalSource g = FIREFLY.traversal();

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

        // Test adding a property to a now empty vertex property's property map
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
    public void testDuplicateVertexPropertyProperties() {
        final GraphTraversalSource g = FIREFLY.traversal();

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
}
