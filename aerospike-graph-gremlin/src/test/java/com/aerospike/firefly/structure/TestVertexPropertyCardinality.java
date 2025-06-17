package com.aerospike.firefly.structure;

import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestVertexPropertyCardinality {
    private static FireflyGraph graph = null;
    private static GraphTraversalSource g = null;

    @BeforeClass
    public static void setUp() {
        graph = FireflyGraph.open(ConfigurationHelper.loadFromFile(INTEGRATION_TEST_PROPERTIES));
        g = graph.traversal();
    }

    @AfterClass
    public static void tearDown() {
        if (graph != null) {
            graph.close();
        }
    }

    @Before
    public void before() {
        g.V().drop().iterate();
    }

    @Test
    public void testVP_SingleStartingValue_Boolean_PropertySingleTrue() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleStartingValue_Boolean_PropertySingleTrue")
                .property(VertexProperty.Cardinality.single, "isBoolean", true)
                .next();
        final List<?> vertices = g.V().has("isBoolean", true).toList();
        Assert.assertEquals(1, vertices.size());
        final List<?> vertices2 = g.V().has("isBoolean", false).toList();
        Assert.assertEquals(0, vertices2.size());
    }

    @Test
    public void testVP_SingleStartingValue_Boolean_PropertySingleFalse() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleStartingValue_Boolean_PropertySingleFalse")
                .property(VertexProperty.Cardinality.single, "isBoolean", false)
                .next();
        final List<?> vertices = g.V().has("isBoolean", false).toList();
        Assert.assertEquals(1, vertices.size());
        final List<?> vertices2 = g.V().has("isBoolean", true).toList();
        Assert.assertEquals(0, vertices2.size());
    }

    @Test
    public void testVP_DoubleStartingValue_Boolean_PropertyListFalse() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_DoubleStartingValue_Boolean_PropertyListFalse")
                .property(VertexProperty.Cardinality.list, "isBoolean", false)
                .property(VertexProperty.Cardinality.list, "isBoolean", false)
                .next();
        final List<?> vertices = g.V().has("isBoolean", false).toList();
        Assert.assertEquals(1, vertices.size());
        final List<?> vertices2 = g.V().has("isBoolean", true).toList();
        Assert.assertEquals(0, vertices2.size());
        final List<? extends Property> properties = g.V(v.id()).properties("isBoolean").toList();
        Assert.assertEquals(2, properties.size());
        Assert.assertTrue(properties.stream().allMatch(p -> p.value().equals(false)));
    }

    @Test
    public void testVP_DoubleStartingValue_Boolean_PropertyListTrue() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_DoubleStartingValue_Boolean_PropertyListFalse")
                .property(VertexProperty.Cardinality.list, "isBoolean", true)
                .property(VertexProperty.Cardinality.list, "isBoolean", true)
                .next();
        final List<?> vertices = g.V().has("isBoolean", true).toList();
        Assert.assertEquals(1, vertices.size());
        final List<?> vertices2 = g.V().has("isBoolean", false).toList();
        Assert.assertEquals(0, vertices2.size());
        final List<? extends Property> properties = g.V(v.id()).properties("isBoolean").toList();
        Assert.assertEquals(2, properties.size());
        Assert.assertTrue(properties.stream().allMatch(p -> p.value().equals(true)));
    }

    @Test
    public void testVP_DoubleStartingValue_Boolean_PropertyListTrueFalse() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_DoubleStartingValue_Boolean_PropertyListTrueFalse")
                .property(VertexProperty.Cardinality.list, "isBoolean", true)
                .property(VertexProperty.Cardinality.list, "isBoolean", false)
                .next();
        final List<?> vertices = g.V().has("isBoolean", true).toList();
        Assert.assertEquals(1, vertices.size());
        final List<?> vertices2 = g.V().has("isBoolean", false).toList();
        Assert.assertEquals(1, vertices2.size());
        final List<? extends Property> properties = g.V(v.id()).properties("isBoolean").toList();
        Assert.assertEquals(2, properties.size());
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals(true)));
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals(false)));
    }

    @Test
    public void testVP_NoStartingValue_Boolean_PropertyListTrue() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_Boolean_PropertyListTrue").next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(0L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.list, "isBoolean", true).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("isBoolean");
        Assert.assertEquals(true, vp.value());
    }

    @Test
    public void testVP_NoStartingValue_Boolean_PropertyListFalse() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_Boolean_PropertyListFalse").next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(0L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.list, "isBoolean", false).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("isBoolean");
        Assert.assertEquals(false, vp.value());
    }

    @Test
    public void testVP_SingleStartingValue_BooleanRemoveTrue() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleStartingValue_BooleanRemoveTrue")
                .property(VertexProperty.Cardinality.single, "isBoolean", true)
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(1L, propertyCount);

        g.V(v.id()).properties("isBoolean").drop().iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(0L, propertyCountAfter);
    }

    @Test
    public void testVP_SingleStartingValue_BooleanRemoveFalse() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleStartingValue_BooleanRemoveFalse")
                .property(VertexProperty.Cardinality.single, "isBoolean", false)
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(1L, propertyCount);

        g.V(v.id()).properties("isBoolean").drop().iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(0L, propertyCountAfter);
    }

    @Test
    public void testDoubleStartingValue_BooleanRemoveTrue() {
        final FireflyVertex v = (FireflyVertex) g.addV("testDoubleStartingValue_BooleanRemoveTrue")
                .property(VertexProperty.Cardinality.list, "isBoolean", true)
                .property(VertexProperty.Cardinality.list, "isBoolean", true)
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(2L, propertyCount);

        g.V(v.id()).properties("isBoolean").drop().iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(0L, propertyCountAfter);
    }


    @Test
    public void testVP_MultiStartingValue_Mixed() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_MultiStartingValue_Mixed")
                .property(VertexProperty.Cardinality.list, "mixed", true)
                .property(VertexProperty.Cardinality.list, "mixed", "yes")
                .property(VertexProperty.Cardinality.list, "mixed", false)
                .property(VertexProperty.Cardinality.list, "mixed", "no")
                .property(VertexProperty.Cardinality.list, "mixed", 1)
                .property(VertexProperty.Cardinality.list, "mixed", Long.MAX_VALUE)
                .property(VertexProperty.Cardinality.list, "mixed", 1.0)
                .next();

        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(7L, propertyCount);
        final List<? extends Property> properties = g.V(v.id()).properties("mixed").toList();
        Assert.assertEquals(7, properties.size());
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals(true)));
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("yes")));
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals(false)));
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("no")));
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals(1)));
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals(Long.MAX_VALUE)));
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals(1.0)));
    }

    @Test
    public void testDoubleStartingValue_BooleanRemoveFalse() {
        final FireflyVertex v = (FireflyVertex) g.addV("testDoubleStartingValue_BooleanRemoveFalse")
                .property(VertexProperty.Cardinality.list, "foo", false)
                .property(VertexProperty.Cardinality.list, "foo", false)
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(2L, propertyCount);

        g.V(v.id()).properties("isBoolean").drop().iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(0L, propertyCountAfter);
    }

    @Test
    public void testDoubleStartingValue_BooleanRemoveTrueFalse() {
        final FireflyVertex v = (FireflyVertex) g.addV("testDoubleStartingValue_BooleanRemoveTrueFalse")
                .property(VertexProperty.Cardinality.list, "isBoolean", true)
                .property(VertexProperty.Cardinality.list, "isBoolean", false)
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(2L, propertyCount);

        g.V(v.id()).properties("isBoolean").drop().iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(0L, propertyCountAfter);
    }

    @Test
    public void testVP_NoStartingValue_BooleanRemoveTrue() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_BooleanRemoveTrue").next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(0L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.list, "isBoolean", true).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("isBoolean");
        Assert.assertEquals(true, vp.value());

        g.V(v.id()).properties("isBoolean").drop().iterate();

        final FireflyVertex vAfter2 = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter2 = IteratorUtils.count(vAfter2.properties());
        Assert.assertEquals(0L, propertyCountAfter2);
    }

    @Test
    public void testVP_NoStartingValue_BooleanRemoveFalse() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_BooleanRemoveFalse").next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(0L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.list, "isBoolean", false).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("isBoolean");
        Assert.assertEquals(false, vp.value());

        g.V(v.id()).properties("isBoolean").drop().iterate();

        final FireflyVertex vAfter2 = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter2 = IteratorUtils.count(vAfter2.properties());
        Assert.assertEquals(0L, propertyCountAfter2);
    }

    @Test
    public void testVP_SingleStartingValue_PropertyList() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleStartingValue_PropertyList")
                .property(VertexProperty.Cardinality.single, "name", "Simon")
                .next();

        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(1L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.list, "name", "Lyndon").iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(2L, propertyCountAfter);
        final List<? extends Property> properties = g.V(v.id()).properties("name").toList();
        Assert.assertEquals(2, properties.size());
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Simon")));
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Lyndon")));
    }

    @Test
    public void testVP_SingleStartingValue_PropertySingle() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleStartingValue_PropertySingle")
                .property(VertexProperty.Cardinality.single, "name", "Simon")
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(1L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.single, "name", "Lyndon").iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty remainingProperty = vAfter.property("name");
        Assert.assertEquals("Lyndon", remainingProperty.value());
    }

    @Test
    public void testVP_SingleStartingValue_NullInsert() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleStartingValue_NullInsert")
                .property(VertexProperty.Cardinality.single, "name", "Simon")
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(1L, propertyCount);

        // Adding a null value should not create a new property in list cardinality, this is a no-op.
        g.V(v.id()).property(VertexProperty.Cardinality.list, "name", null).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("name");
        Assert.assertEquals("Simon", vp.value());
    }

    @Test
    public void testVP_NoStartingValue_PropertyList() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_PropertyList").next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(0L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.list, "name", "Lyndon").iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("name");
        Assert.assertEquals("Lyndon", vp.value());

        g.V(v.id()).property(VertexProperty.Cardinality.list, "name", "Simon").iterate();

        final FireflyVertex vAfter2 = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter2 = IteratorUtils.count(vAfter2.properties());
        Assert.assertEquals(2L, propertyCountAfter2);
        final List<? extends Property> properties = g.V(v.id()).properties("name").toList();
        Assert.assertEquals(2, properties.size());
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Lyndon")));
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Simon")));
    }

    @Test
    public void testVP_NoStartingValue_PropertySingle() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_PropertySingle").next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(0L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.single, "name", "Lyndon").iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("name");
        Assert.assertEquals("Lyndon", vp.value());
    }


    @Test
    public void testVPC_MultipleStartingValues_DoubleValueCheck() {
        g.addV("testVPC_MultipleStartingValues_ValueCheck")
                .property(VertexProperty.Cardinality.list, "name", "Lyndon")
                .property(VertexProperty.Cardinality.list,"name", "Simon")
                .next();
        final long propertyCount = IteratorUtils.count(g.V().hasLabel("testVPC_MultipleStartingValues_ValueCheck").properties());
        Assert.assertEquals(2L, propertyCount);

        final List<? extends Property> properties = g.V().hasLabel("testVPC_MultipleStartingValues_ValueCheck").properties("name").toList();
        Assert.assertEquals(2, properties.size());
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Lyndon")));
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Simon")));
    }

    @Test
    public void testVPC_MultipleStartingValues_SingleValueCheckSingle() {
        g.addV("testVPC_MultipleStartingValues_SingleValueCheck")
                .property(VertexProperty.Cardinality.single, "name", "Lyndon")
                .next();
        final long propertyCount = IteratorUtils.count(g.V().hasLabel("testVPC_MultipleStartingValues_SingleValueCheck").properties());
        Assert.assertEquals(1L, propertyCount);

        final List<? extends Property> properties = g.V().hasLabel("testVPC_MultipleStartingValues_SingleValueCheck").properties("name").toList();
        Assert.assertEquals(1, properties.size());
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Lyndon")));
    }

    @Test
    public void testVPC_MultipleStartingValues_DoubleValueSingleList() {
        g.addV("testVPC_MultipleStartingValues_SingleValueCheck")
                .property(VertexProperty.Cardinality.single, "name", "Simon")
                .property(VertexProperty.Cardinality.list, "name", "Lyndon")
                .next();
        final long propertyCount = IteratorUtils.count(g.V().hasLabel("testVPC_MultipleStartingValues_SingleValueCheck").properties());
        Assert.assertEquals(2L, propertyCount);

        final List<? extends Property> properties = g.V().hasLabel("testVPC_MultipleStartingValues_SingleValueCheck").properties("name").toList();
        Assert.assertEquals(2, properties.size());
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Lyndon")));
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Simon")));
    }

    @Test
    public void testVPC_MultipleStartingValues_DoubleValueListSingle() {
        g.addV("testVPC_MultipleStartingValues_DoubleValueListSingle")
                .property(VertexProperty.Cardinality.list, "name", "Lyndon")
                .property(VertexProperty.Cardinality.single, "name", "Simon")
                .next();
        final long propertyCount = IteratorUtils.count(g.V().hasLabel("testVPC_MultipleStartingValues_DoubleValueListSingle").properties());
        Assert.assertEquals(1L, propertyCount);

        final List<? extends Property> properties = g.V().hasLabel("testVPC_MultipleStartingValues_DoubleValueListSingle").properties("name").toList();
        Assert.assertEquals(1, properties.size());
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Simon")));
    }

    @Test
    public void testVPC_MultipleStartingValues_SingleValueCheckList() {
        g.addV("testVPC_MultipleStartingValues_SingleValueCheckList")
                .property(VertexProperty.Cardinality.list, "name", "Lyndon")
                .next();
        final long propertyCount = IteratorUtils.count(g.V().hasLabel("testVPC_MultipleStartingValues_SingleValueCheckList").properties());
        Assert.assertEquals(1L, propertyCount);

        final List<? extends Property> properties = g.V().hasLabel("testVPC_MultipleStartingValues_SingleValueCheckList").properties("name").toList();
        Assert.assertEquals(1, properties.size());
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Lyndon")));
    }


    @Test
    public void testVPC_MultipleStartingValues_SinglePropertyNull() {
        g.addV("testVPC_MultipleStartingValues_SinglePropertyNull")
                .property(VertexProperty.Cardinality.list, "name", "Lyndon")
                .property(VertexProperty.Cardinality.list,"name", "Simon")
                .next();
        final long propertyCount = IteratorUtils.count(g.V().hasLabel("testVPC_MultipleStartingValues_SinglePropertyNull").properties());
        Assert.assertEquals(2L, propertyCount);

        g.V().hasLabel("testVPC_MultipleStartingValues_SinglePropertyNull").
                property("name", null).iterate();

        final long propertyCountAfter1 = IteratorUtils.count(
                g.V().hasLabel("testVPC_MultipleStartingValues_SinglePropertyNull").next().properties("name"));
        Assert.assertEquals(0L, propertyCountAfter1);
        final long propertyCountAfter2 = g.V().hasLabel("testVPC_MultipleStartingValues_SinglePropertyNull").properties("name").count().next();
        Assert.assertEquals(0L, propertyCountAfter2);
    }


    @Test
    public void testVPC_MultipleStartingValues_ListPropertyNull() {
        g.addV("testVPC_MultipleStartingValues_ListPropertyNull")
                .property(VertexProperty.Cardinality.list, "name", "Lyndon")
                .property(VertexProperty.Cardinality.list,"name", "Simon")
                .next();
        final long propertyCount = IteratorUtils.count(g.V().hasLabel("testVPC_MultipleStartingValues_ListPropertyNull").properties());
        Assert.assertEquals(2L, propertyCount);

        g.V().hasLabel("testVPC_MultipleStartingValues_ListPropertyNull").
                property(VertexProperty.Cardinality.list, "name", null).iterate();

        final long propertyCountAfter1 = IteratorUtils.count(
                g.V().hasLabel("testVPC_MultipleStartingValues_ListPropertyNull").next().properties("name"));
        Assert.assertEquals(2L, propertyCountAfter1);
        final long propertyCountAfter2 = g.V().hasLabel("testVPC_MultipleStartingValues_ListPropertyNull").properties("name").count().next();
        Assert.assertEquals(2L, propertyCountAfter2);
    }

    @Test
    public void testVPC_MultipleStartingValues_PropertiesDrop() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVPC_MultipleStartingValues_PropertiesDrop")
                .property(VertexProperty.Cardinality.list, "name", "Lyndon")
                .property(VertexProperty.Cardinality.list,"name", "Simon")
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(2L, propertyCount);

        g.V().hasLabel("testVPC_MultipleStartingValues_PropertiesDrop").
                properties("name").drop().iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter1 = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(0L, propertyCountAfter1);
        final long propertyCountAfter2 = g.V(v.id()).properties("name").count().next();
        Assert.assertEquals(0L, propertyCountAfter2);
    }

    @Test
    public void testVPC_MultipleStartingValues_hasIdDrop() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVPC_MultipleStartingValues_hasIdDrop")
                .property(VertexProperty.Cardinality.list, "name", "Lyndon")
                .property(VertexProperty.Cardinality.list,"name", "Simon")
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(2L, propertyCount);

        VertexProperty dropLyndon = null;
        final Iterator<VertexProperty<Object>> it = v.properties("name");
        while (it.hasNext()) {
            final VertexProperty vp = it.next();
            if (vp.value().equals("Lyndon")) {
                dropLyndon = vp;
                break;
            }
        }
        Assert.assertNotNull(dropLyndon);
        g.V().hasLabel("testVPC_MultipleStartingValues_hasIdDrop").
                properties("name").hasId(dropLyndon.id()).drop().iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty remainingProperty = vAfter.properties("name").next();
        Assert.assertEquals("Simon", remainingProperty.value());
        final VertexProperty remainingProperty2 = vAfter.property("name");
        Assert.assertEquals("Simon", remainingProperty2.value());
        final List<? extends Property> remainingProperty3 = g.V(v.id()).properties("name").toList();
        Assert.assertEquals(1, remainingProperty3.size());
        Assert.assertEquals("Simon", remainingProperty3.get(0).value());
    }

    @Test
    public void testVPC_MultipleStartingValues_whereValuesDrop() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVPC_MultipleStartingValues_whereValuesDrop")
                .property(VertexProperty.Cardinality.list, "name", "Lyndon")
                .property(VertexProperty.Cardinality.list,"name", "Simon")
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(2L, propertyCount);

        g.V().hasLabel("testVPC_MultipleStartingValues_whereValuesDrop").
                properties("name").where(__.value().is(P.eq("Lyndon"))).drop().iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty remainingProperty = vAfter.properties("name").next();
        Assert.assertEquals("Simon", remainingProperty.value());
        final VertexProperty remainingProperty2 = vAfter.property("name");
        Assert.assertEquals("Simon", remainingProperty2.value());
        final List<? extends Property> remainingProperty3 = g.V(v.id()).properties("name").toList();
        Assert.assertEquals(1, remainingProperty3.size());
        Assert.assertEquals("Simon", remainingProperty3.get(0).value());
    }
}
