package com.aerospike.firefly.structure;

import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.commons.configuration2.Configuration;
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

    // TODO: Test VPPS!!!!

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


    //    We do not have an existing Vertex
    //        g.addV(<id>).property(Cardinality.List, “name”, “Lyndon”).property(Cardinality.List, “name”, “Simon”)
    //            Handled on initial vertex write.
    //        g.addV(<id>).property(Cardinality.List, “name”, “Lyndon”)
    //            Handled on initial vertex write.
    //        g.addV(<id>).property(Cardinality.Single, “name”, “Lyndon”)
    //            Handled on initial vertex write.
    //        g.addV(<id>).property(Cardinality.List, “name”, “Lyndon”).property(Cardinality.Single, “name”, “Simon”)
    //            Is this an error or just overwrite w/ Simon?
    //    Special case:
    //        g.V(<id>).property(Cardinality.List, “name”, null)
    //            No-op because we do not support null property values. If we did this would insert null.
    //    Removal of property with single value
    //        g.V(<id>).properties(“name”).drop().iterate()
    //            Remove “name” entry from VPB
    //            Remove “name” entry of VPPB

    @Test
    public void testVPC_MultipleStartingValues_PropertyNull() {
        g.addV("testVPC_MultipleStartingValues_PropertyDrop")
                .property(VertexProperty.Cardinality.list, "name", "Lyndon")
                .property(VertexProperty.Cardinality.list,"name", "Simon")
                .next();
        final long propertyCount = IteratorUtils.count(g.V().hasLabel("testVPC_MultipleStartingValues_PropertyDrop").properties());
        Assert.assertEquals(2L, propertyCount);

        g.V().hasLabel("testVPC_MultipleStartingValues_PropertyDrop").
                property("name", null).iterate();

        final long propertyCountAfter1 = IteratorUtils.count(
                g.V().hasLabel("testVPC_MultipleStartingValues_PropertyDrop").next().properties("name"));
        Assert.assertEquals(0L, propertyCountAfter1);
        final long propertyCountAfter2 = g.V().hasLabel("testVPC_MultipleStartingValues_PropertyDrop").properties("name").count().next();
        Assert.assertEquals(0L, propertyCountAfter2);
    }

    @Test
    public void testVPC_MultipleStartingValues_PropertiesDrop() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVPC_MultipleStartingValues_hasIdDrop")
                .property(VertexProperty.Cardinality.list, "name", "Lyndon")
                .property(VertexProperty.Cardinality.list,"name", "Simon")
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(2L, propertyCount);

        g.V().hasLabel("testVPC_MultipleStartingValues_hasIdDrop").
                properties("name").drop().iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter1 = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(0L, propertyCountAfter1);
        final long propertyCountAfter2 = g.V(v.id()).properties("name").count().next();
        Assert.assertEquals(0L, propertyCountAfter2);
    }

    @Test
    public void testVPC_MultipleStartingValues_hasIdDrop() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVPC_MultipleStartingValues_whereValuesDrop")
                .property(VertexProperty.Cardinality.list, "name", "Lyndon")
                .property(VertexProperty.Cardinality.list,"name", "Simon")
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(2L, propertyCount);

        VertexProperty dropLyndon = null;
        while (v.properties("name").hasNext()) {
            final VertexProperty vp = v.properties("name").next();
            if (vp.value().equals("Lyndon")) {
                dropLyndon = vp;
                break;
            }
        }
        Assert.assertNotNull(dropLyndon);
        g.V().hasLabel("testVPC_MultipleStartingValues_whereValuesDrop").
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
        final FireflyVertex v = (FireflyVertex) g.addV("testVPC_MultipleStartingValues_hasIdDrop")
                .property(VertexProperty.Cardinality.list, "name", "Lyndon")
                .property(VertexProperty.Cardinality.list,"name", "Simon")
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(2L, propertyCount);

        g.V().hasLabel("testVPC_MultipleStartingValues_hasIdDrop").
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
