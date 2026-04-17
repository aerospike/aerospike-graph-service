/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.structure;

import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.exceptions.AerospikeGraphException;
import com.aerospike.firefly.util.exceptions.GraphError;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
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

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

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

    // Double data type tests

    @Test
    public void testVP_SingleStartingValue_Double_PropertySingle() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleStartingValue_Double_PropertySingle")
                .property(VertexProperty.Cardinality.single, "value", 1.0)
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(1L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.single, "value", 2.0).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("value");
        Assert.assertEquals(2.0, vp.value());
    }

    @Test
    public void testVP_SingleStartingValue_Double_PropertyList() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleStartingValue_Double_PropertyList")
                .property(VertexProperty.Cardinality.list, "value", 1.0)
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(1L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.list, "value", 2.0).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(2L, propertyCountAfter);
        final List<? extends Property> properties = g.V(v.id()).properties("value").toList();
        Assert.assertEquals(2, properties.size());
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals(1.0)));
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals(2.0)));
    }

    @Test
    public void testVP_NoStartingValue_Double_PropertyList() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_Double_PropertyList").next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(0L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.list, "value", 1.0).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("value");
        Assert.assertEquals(1.0, vp.value());
    }

    @Test
    public void testVP_NoStartingValue_Double_PropertySingle() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_Double_PropertySingle").next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(0L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.single, "value", 1.0).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("value");
        Assert.assertEquals(1.0, vp.value());
    }

    @Test
    public void testVP_MultiStartingValue_Double() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_MultiStartingValue_Double")
                .property(VertexProperty.Cardinality.list, "value", 1.0)
                .property(VertexProperty.Cardinality.list, "value", 2.0)
                .next();

        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(2L, propertyCount);
        final List<? extends Property> properties = g.V(v.id()).properties("value").toList();
        Assert.assertEquals(2, properties.size());
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals(1.0)));
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals(2.0)));
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
                .property(VertexProperty.Cardinality.list, "isBoolean", false)
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
                .property(VertexProperty.Cardinality.single, "name", "Bob")
                .next();

        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(1L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.list, "name", "Alice").iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(2L, propertyCountAfter);
        final List<? extends Property> properties = g.V(v.id()).properties("name").toList();
        Assert.assertEquals(2, properties.size());
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Bob")));
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Alice")));
    }

    @Test
    public void testVP_SingleStartingValue_PropertySingle() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleStartingValue_PropertySingle")
                .property(VertexProperty.Cardinality.single, "name", "Bob")
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(1L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.single, "name", "Alice").iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty remainingProperty = vAfter.property("name");
        Assert.assertEquals("Alice", remainingProperty.value());
    }

    @Test
    public void testVP_SingleStartingValue_NullInsert() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleStartingValue_NullInsert")
                .property(VertexProperty.Cardinality.single, "name", "Bob")
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(1L, propertyCount);

        // Adding a null value should not create a new property in list cardinality, this is a no-op.
        g.V(v.id()).property(VertexProperty.Cardinality.list, "name", null).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("name");
        Assert.assertEquals("Bob", vp.value());
    }

    @Test
    public void testVP_NoStartingValue_PropertyList() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_PropertyList").next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(0L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.list, "name", "Alice").iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("name");
        Assert.assertEquals("Alice", vp.value());

        g.V(v.id()).property(VertexProperty.Cardinality.list, "name", "Bob").iterate();

        final FireflyVertex vAfter2 = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter2 = IteratorUtils.count(vAfter2.properties());
        Assert.assertEquals(2L, propertyCountAfter2);
        final List<? extends Property> properties = g.V(v.id()).properties("name").toList();
        Assert.assertEquals(2, properties.size());
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Alice")));
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Bob")));
    }

    @Test
    public void testVP_NoStartingValue_PropertySingle() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_PropertySingle").next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(0L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.single, "name", "Alice").iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("name");
        Assert.assertEquals("Alice", vp.value());
    }


    @Test
    public void testVPC_MultipleStartingValues_DoubleValueCheck() {
        g.addV("testVPC_MultipleStartingValues_ValueCheck")
                .property(VertexProperty.Cardinality.list, "name", "Alice")
                .property(VertexProperty.Cardinality.list,"name", "Bob")
                .next();
        final long propertyCount = IteratorUtils.count(g.V().hasLabel("testVPC_MultipleStartingValues_ValueCheck").properties());
        Assert.assertEquals(2L, propertyCount);

        final List<? extends Property> properties = g.V().hasLabel("testVPC_MultipleStartingValues_ValueCheck").properties("name").toList();
        Assert.assertEquals(2, properties.size());
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Alice")));
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Bob")));
    }

    @Test
    public void testVPC_MultipleStartingValues_SingleValueCheckSingle() {
        g.addV("testVPC_MultipleStartingValues_SingleValueCheckSingle")
                .property(VertexProperty.Cardinality.single,"name", "Bob")
                .property(VertexProperty.Cardinality.list, "name", "Alice")
                .property(VertexProperty.Cardinality.list, "name", "Connor")
                .next();
        long propertyCount = IteratorUtils.count(g.V().hasLabel("testVPC_MultipleStartingValues_SingleValueCheckSingle").properties());
        Assert.assertEquals(3L, propertyCount);

        List<? extends Property> properties = g.V().hasLabel("testVPC_MultipleStartingValues_SingleValueCheckSingle").properties("name").toList();
        Assert.assertEquals(3, properties.size());
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Bob")));
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Alice")));
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Connor")));

        g.V().hasLabel("testVPC_MultipleStartingValues_SingleValueCheckSingle").property(VertexProperty.Cardinality.single, "name", "Valentyn").iterate();
        propertyCount = IteratorUtils.count(g.V().hasLabel("testVPC_MultipleStartingValues_SingleValueCheckSingle").properties());
        Assert.assertEquals(1L, propertyCount);

        properties = g.V().hasLabel("testVPC_MultipleStartingValues_SingleValueCheckSingle").properties("name").toList();
        Assert.assertEquals(1, properties.size());
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Valentyn")));
    }

    @Test
    public void testVPC_MultipleStartingValues_DoubleValueSingleList() {
        g.addV("testVPC_MultipleStartingValues_DoubleValueSingleList")
                .property(VertexProperty.Cardinality.single, "name", "Bob")
                .property(VertexProperty.Cardinality.list, "name", "Alice")
                .next();
        final long propertyCount = IteratorUtils.count(g.V().hasLabel("testVPC_MultipleStartingValues_DoubleValueSingleList").properties());
        Assert.assertEquals(2L, propertyCount);

        final List<? extends Property> properties = g.V().hasLabel("testVPC_MultipleStartingValues_DoubleValueSingleList").properties("name").toList();
        Assert.assertEquals(2, properties.size());
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Alice")));
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Bob")));
    }

    @Test
    public void testVPC_MultipleStartingValues_DoubleValueListSingle() {
        g.addV("testVPC_MultipleStartingValues_DoubleValueListSingle")
                .property(VertexProperty.Cardinality.list, "name", "Alice")
                .property(VertexProperty.Cardinality.single, "name", "Bob")
                .next();
        final long propertyCount = IteratorUtils.count(g.V().hasLabel("testVPC_MultipleStartingValues_DoubleValueListSingle").properties());
        Assert.assertEquals(1L, propertyCount);

        final List<? extends Property> properties = g.V().hasLabel("testVPC_MultipleStartingValues_DoubleValueListSingle").properties("name").toList();
        Assert.assertEquals(1, properties.size());
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Bob")));
    }

    @Test
    public void testVPC_MultipleStartingValues_SingleValueCheckList() {
        g.addV("testVPC_MultipleStartingValues_SingleValueCheckList")
                .property(VertexProperty.Cardinality.list, "name", "Alice")
                .next();
        final long propertyCount = IteratorUtils.count(g.V().hasLabel("testVPC_MultipleStartingValues_SingleValueCheckList").properties());
        Assert.assertEquals(1L, propertyCount);

        final List<? extends Property> properties = g.V().hasLabel("testVPC_MultipleStartingValues_SingleValueCheckList").properties("name").toList();
        Assert.assertEquals(1, properties.size());
        Assert.assertTrue(properties.stream().anyMatch(p -> p.value().equals("Alice")));
    }


    @Test
    public void testVPC_MultipleStartingValues_SinglePropertyNull() {
        g.addV("testVPC_MultipleStartingValues_SinglePropertyNull")
                .property(VertexProperty.Cardinality.list, "name", "Alice")
                .property(VertexProperty.Cardinality.list,"name", "Bob")
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
                .property(VertexProperty.Cardinality.list, "name", "Alice")
                .property(VertexProperty.Cardinality.list,"name", "Bob")
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
                .property(VertexProperty.Cardinality.list, "name", "Alice")
                .property(VertexProperty.Cardinality.list,"name", "Bob")
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
    public void testVPC_MultipleStartingValues_HasIdDrop() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVPC_MultipleStartingValues_HasIdDrop")
                .property(VertexProperty.Cardinality.list, "name", "Alice")
                .property(VertexProperty.Cardinality.list,"name", "Bob")
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(2L, propertyCount);

        VertexProperty dropAlice = null;
        final Iterator<VertexProperty<Object>> it = v.properties("name");
        while (it.hasNext()) {
            final VertexProperty vp = it.next();
            if (vp.value().equals("Alice")) {
                dropAlice = vp;
                break;
            }
        }
        Assert.assertNotNull(dropAlice);
        g.V().hasLabel("testVPC_MultipleStartingValues_HasIdDrop").
                properties("name").hasId(dropAlice.id()).drop().iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty remainingProperty = vAfter.properties("name").next();
        Assert.assertEquals("Bob", remainingProperty.value());
        final VertexProperty remainingProperty2 = vAfter.property("name");
        Assert.assertEquals("Bob", remainingProperty2.value());
        final List<? extends Property> remainingProperty3 = g.V(v.id()).properties("name").toList();
        Assert.assertEquals(1, remainingProperty3.size());
        Assert.assertEquals("Bob", remainingProperty3.get(0).value());
    }

    @Test
    public void testVPC_MultipleStartingValues_WhereValuesDrop() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVPC_MultipleStartingValues_WhereValuesDrop")
                .property(VertexProperty.Cardinality.list, "name", "Alice")
                .property(VertexProperty.Cardinality.list,"name", "Bob")
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(2L, propertyCount);

        g.V().hasLabel("testVPC_MultipleStartingValues_WhereValuesDrop").
                properties("name").where(__.value().is(P.eq("Alice"))).drop().iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty remainingProperty = vAfter.properties("name").next();
        Assert.assertEquals("Bob", remainingProperty.value());
        final VertexProperty remainingProperty2 = vAfter.property("name");
        Assert.assertEquals("Bob", remainingProperty2.value());
        final List<? extends Property> remainingProperty3 = g.V(v.id()).properties("name").toList();
        Assert.assertEquals(1, remainingProperty3.size());
        Assert.assertEquals("Bob", remainingProperty3.get(0).value());
    }

    @Test
    public void testVP_NoStartingValue_PropertySetInt() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_PropertySetInt").next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(0L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("foo");
        Assert.assertEquals(1, vp.value());
    }

    @Test
    public void testVP_NoStartingValue_PropertySetLong() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_PropertySetInt").next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(0L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("foo");
        Assert.assertEquals(1, vp.value());
    }

    @Test
    public void testVP_NoStartingValue_PropertySetDouble() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_PropertySetDouble").next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(0L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1.1).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("foo");
        Assert.assertEquals(1.1, vp.value());
    }

    @Test
    public void testVP_NoStartingValue_PropertySetString() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_PropertySetString").next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(0L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", "1").iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("foo");
        Assert.assertEquals("1", vp.value());
    }

    @Test
    public void testVP_NoStartingValue_PropertySetBool() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_PropertySetBool").next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(0L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", true).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("foo");
        Assert.assertEquals(true, vp.value());
    }

    @Test
    public void testVP_NoStartingValue_PropertySetDate() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_PropertySetDate").next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(0L, propertyCount);

        final Date date = new Date(1672531200000L);
        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", date).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("foo");
        Assert.assertEquals(date, vp.value());
    }

    @Test
    public void testVP_SingleMatchStartingValue_PropertySetInt() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleMatchStartingValue_PropertySetInt")
                .property("foo", 1).next();
        final Iterator<VertexProperty<Object>> propertyItty = v.properties();
        Assert.assertTrue(propertyItty.hasNext());
        final VertexProperty<Object> property = propertyItty.next();
        Assert.assertFalse(propertyItty.hasNext());

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("foo");
        Assert.assertEquals(1, vp.value());
        Assert.assertEquals(property.id(), vp.id());
    }

    @Test
    public void testVP_SingleMatchStartingValue_PropertySetLong() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleMatchStartingValue_PropertySetLong")
                .property("foo", 1L).next();
        final Iterator<VertexProperty<Object>> propertyItty = v.properties();
        Assert.assertTrue(propertyItty.hasNext());
        final VertexProperty<Object> property = propertyItty.next();
        Assert.assertFalse(propertyItty.hasNext());

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1L).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("foo");
        Assert.assertEquals(1L, vp.value());
        Assert.assertEquals(property.id(), vp.id());
    }

    @Test
    public void testVP_SingleMatchStartingValue_PropertySetDouble() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleMatchStartingValue_PropertySetDouble")
                .property("foo", 1.1).next();
        final Iterator<VertexProperty<Object>> propertyItty = v.properties();
        Assert.assertTrue(propertyItty.hasNext());
        final VertexProperty<Object> property = propertyItty.next();
        Assert.assertFalse(propertyItty.hasNext());

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1.1).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("foo");
        Assert.assertEquals(1.1, vp.value());
        Assert.assertEquals(property.id(), vp.id());
    }

    @Test
    public void testVP_SingleMatchStartingValue_PropertySetString() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleMatchStartingValue_PropertySetString")
                .property("foo", "1").next();
        final Iterator<VertexProperty<Object>> propertyItty = v.properties();
        Assert.assertTrue(propertyItty.hasNext());
        final VertexProperty<Object> property = propertyItty.next();
        Assert.assertFalse(propertyItty.hasNext());

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", "1").iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("foo");
        Assert.assertEquals("1", vp.value());
        Assert.assertEquals(property.id(), vp.id());
    }

    @Test
    public void testVP_SingleMatchStartingValue_PropertySetBool() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleMatchStartingValue_PropertySetBool")
                .property("foo", true).next();
        final Iterator<VertexProperty<Object>> propertyItty = v.properties();
        Assert.assertTrue(propertyItty.hasNext());
        final VertexProperty<Object> property = propertyItty.next();
        Assert.assertFalse(propertyItty.hasNext());

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", true).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("foo");
        Assert.assertEquals(true, vp.value());
        Assert.assertEquals(property.id(), vp.id());
    }

    @Test
    public void testVP_SingleMatchStartingValue_PropertySetDate() {
        final Date date = new Date(1672531200000L);
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleMatchStartingValue_PropertySetDate")
                .property("foo", date).next();
        final Iterator<VertexProperty<Object>> propertyItty = v.properties();
        Assert.assertTrue(propertyItty.hasNext());
        final VertexProperty<Object> property = propertyItty.next();
        Assert.assertFalse(propertyItty.hasNext());

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", date).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("foo");
        Assert.assertEquals(date, vp.value());
        Assert.assertEquals(property.id(), vp.id());
    }

    @Test
    public void testVP_SingleMismatchStartingValue_PropertySetInt() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleMismatchStartingValue_PropertySetInt")
                .property("foo", 2).next();
        final Iterator<VertexProperty<Object>> propertyItty = v.properties();
        Assert.assertTrue(propertyItty.hasNext());
        final VertexProperty<Object> property = propertyItty.next();
        Assert.assertFalse(propertyItty.hasNext());

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1).next();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(2L, propertyCountAfter);
        final Iterator<VertexProperty<Object>> vps = vAfter.properties("foo");
        boolean found = false;
        while (vps.hasNext()) {
            final VertexProperty<Object> vp = vps.next();
            if (vp.value().equals(1)) {
                if (found) {
                    Assert.fail("Found duplicate matching set values.");
                }
                found = true;
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testVP_SingleMismatchStartingValue_PropertySetLong() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleMismatchStartingValue_PropertySetLong")
                .property("foo", 2L).next();
        final Iterator<VertexProperty<Object>> propertyItty = v.properties();
        Assert.assertTrue(propertyItty.hasNext());
        final VertexProperty<Object> property = propertyItty.next();
        Assert.assertFalse(propertyItty.hasNext());

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1L).next();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(2L, propertyCountAfter);
        final Iterator<VertexProperty<Object>> vps = vAfter.properties("foo");
        boolean found = false;
        while (vps.hasNext()) {
            final VertexProperty<Object> vp = vps.next();
            if (vp.value().equals(1L)) {
                if (found) {
                    Assert.fail("Found duplicate matching set values.");
                }
                found = true;
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testVP_SingleMismatchStartingValue_PropertySetDouble() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleMismatchStartingValue_PropertySetDouble")
                .property("foo", 1.2).next();
        final Iterator<VertexProperty<Object>> propertyItty = v.properties();
        Assert.assertTrue(propertyItty.hasNext());
        final VertexProperty<Object> property = propertyItty.next();
        Assert.assertFalse(propertyItty.hasNext());

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1.1).next();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(2L, propertyCountAfter);
        final Iterator<VertexProperty<Object>> vps = vAfter.properties("foo");
        boolean found = false;
        while (vps.hasNext()) {
            final VertexProperty<Object> vp = vps.next();
            if (vp.value().equals(1.1)) {
                if (found) {
                    Assert.fail("Found duplicate matching set values.");
                }
                found = true;
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testVP_SingleMismatchStartingValue_PropertySetString() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleMismatchStartingValue_PropertySetString")
                .property("foo", "bar").next();
        final Iterator<VertexProperty<Object>> propertyItty = v.properties();
        Assert.assertTrue(propertyItty.hasNext());
        final VertexProperty<Object> property = propertyItty.next();
        Assert.assertFalse(propertyItty.hasNext());

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", "baz").next();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(2L, propertyCountAfter);
        final Iterator<VertexProperty<Object>> vps = vAfter.properties("foo");
        boolean found = false;
        while (vps.hasNext()) {
            final VertexProperty<Object> vp = vps.next();
            if (vp.value().equals("baz")) {
                if (found) {
                    Assert.fail("Found duplicate matching set values.");
                }
                found = true;
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testVP_SingleMismatchStartingValue_PropertySetBool() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleMismatchStartingValue_PropertySetBool")
                .property("foo", false).next();
        final Iterator<VertexProperty<Object>> propertyItty = v.properties();
        Assert.assertTrue(propertyItty.hasNext());
        final VertexProperty<Object> property = propertyItty.next();
        Assert.assertFalse(propertyItty.hasNext());

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", true).next();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(2L, propertyCountAfter);
        final Iterator<VertexProperty<Object>> vps = vAfter.properties("foo");
        boolean found = false;
        while (vps.hasNext()) {
            final VertexProperty<Object> vp = vps.next();
            if (vp.value().equals(true)) {
                if (found) {
                    Assert.fail("Found duplicate matching set values.");
                }
                found = true;
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testVP_SingleMismatchStartingValue_PropertySetDate() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_SingleMismatchStartingValue_PropertySetDate")
                .property("foo", new Date(1627531200000L)).next();
        final Iterator<VertexProperty<Object>> propertyItty = v.properties();
        Assert.assertTrue(propertyItty.hasNext());
        final VertexProperty<Object> property = propertyItty.next();
        Assert.assertFalse(propertyItty.hasNext());

        final Date date = new Date(1672531200000L);
        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", date).next();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(2L, propertyCountAfter);
        final Iterator<VertexProperty<Object>> vps = vAfter.properties("foo");
        boolean found = false;
        while (vps.hasNext()) {
            final VertexProperty<Object> vp = vps.next();
            if (vp.value().equals(date)) {
                if (found) {
                    Assert.fail("Found duplicate matching set values.");
                }
                found = true;
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testVP_MultiMatchStartingValue_PropertySetInt() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_MultiMatchStartingValue_PropertySetInt")
                .property("foo", 1).next();
        g.V(v.id()).property(VertexProperty.Cardinality.list, "foo", 2).iterate();
        Assert.assertEquals(2L, IteratorUtils.count(g.V(v.id()).properties()));

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 2).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(2L, propertyCountAfter);
        final Iterator<VertexProperty<Object>> vps = vAfter.properties("foo");
        boolean found = false;
        while (vps.hasNext()) {
            final VertexProperty<Object> vp = vps.next();
            if (vp.value().equals(2)) {
                if (found) {
                    Assert.fail("Found duplicate matching set values.");
                }
                found = true;
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testVP_MultiMatchStartingValue_PropertySetLong() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_MultiMatchStartingValue_PropertySetLong")
                .property("foo", 1L).next();
        g.V(v.id()).property(VertexProperty.Cardinality.list, "foo", 2L).iterate();
        Assert.assertEquals(2L, IteratorUtils.count(g.V(v.id()).properties()));

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 2L).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(2L, propertyCountAfter);
        final Iterator<VertexProperty<Object>> vps = vAfter.properties("foo");
        boolean found = false;
        while (vps.hasNext()) {
            final VertexProperty<Object> vp = vps.next();
            if (vp.value().equals(2L)) {
                if (found) {
                    Assert.fail("Found duplicate matching set values.");
                }
                found = true;
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testVP_MultiMatchStartingValue_PropertySetDouble() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_MultiMatchStartingValue_PropertySetDouble")
                .property("foo", 1.1).next();
        g.V(v.id()).property(VertexProperty.Cardinality.list, "foo", 1.2).iterate();
        Assert.assertEquals(2L, IteratorUtils.count(g.V(v.id()).properties()));

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1.2).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(2L, propertyCountAfter);
        final Iterator<VertexProperty<Object>> vps = vAfter.properties("foo");
        boolean found = false;
        while (vps.hasNext()) {
            final VertexProperty<Object> vp = vps.next();
            if (vp.value().equals(1.2)) {
                if (found) {
                    Assert.fail("Found duplicate matching set values.");
                }
                found = true;
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testVP_MultiMatchStartingValue_PropertySetString() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_MultiMatchStartingValue_PropertySetString")
                .property("foo", "1").next();
        g.V(v.id()).property(VertexProperty.Cardinality.list, "foo", "2").iterate();
        Assert.assertEquals(2L, IteratorUtils.count(g.V(v.id()).properties()));

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", "2").iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(2L, propertyCountAfter);
        final Iterator<VertexProperty<Object>> vps = vAfter.properties("foo");
        boolean found = false;
        while (vps.hasNext()) {
            final VertexProperty<Object> vp = vps.next();
            if (vp.value().equals("2")) {
                if (found) {
                    Assert.fail("Found duplicate matching set values.");
                }
                found = true;
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testVP_MultiMatchStartingValue_PropertySetBool() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_MultiMatchStartingValue_PropertySetBool")
                .property("foo", true).next();
        g.V(v.id()).property(VertexProperty.Cardinality.list, "foo", false).iterate();
        Assert.assertEquals(2L, IteratorUtils.count(g.V(v.id()).properties()));

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", false).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(2L, propertyCountAfter);
        final Iterator<VertexProperty<Object>> vps = vAfter.properties("foo");
        boolean found = false;
        while (vps.hasNext()) {
            final VertexProperty<Object> vp = vps.next();
            if (vp.value().equals(false)) {
                if (found) {
                    Assert.fail("Found duplicate matching set values.");
                }
                found = true;
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testVP_MultiMatchStartingValue_PropertySetDate() {
        final Date date = new Date(1672531200000L);
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_MultiMatchStartingValue_PropertySetDate")
                .property("foo", new Date(1627531200000L)).next();
        g.V(v.id()).property(VertexProperty.Cardinality.list, "foo", date).iterate();
        Assert.assertEquals(2L, IteratorUtils.count(g.V(v.id()).properties()));

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", date).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(2L, propertyCountAfter);
        final Iterator<VertexProperty<Object>> vps = vAfter.properties("foo");
        boolean found = false;
        while (vps.hasNext()) {
            final VertexProperty<Object> vp = vps.next();
            if (vp.value().equals(date)) {
                if (found) {
                    Assert.fail("Found duplicate matching set values.");
                }
                found = true;
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testVP_PredicateMatchValue_PropertySetIntLong() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_PredicateMatchValue_PropertySetIntLong")
                .property("foo", 1).next();
        final Iterator<VertexProperty<Object>> propertyItty = v.properties();
        Assert.assertTrue(propertyItty.hasNext());
        final VertexProperty<Object> property = propertyItty.next();
        Assert.assertFalse(propertyItty.hasNext());

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1L).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("foo");
        Assert.assertEquals(1, vp.value());
        Assert.assertEquals(property.id(), vp.id());
    }

    @Test
    public void testVP_PredicateMatchValue_PropertySetLongInt() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_PredicateMatchValue_PropertySetIntLong")
                .property("foo", 1L).next();
        final Iterator<VertexProperty<Object>> propertyItty = v.properties();
        Assert.assertTrue(propertyItty.hasNext());
        final VertexProperty<Object> property = propertyItty.next();
        Assert.assertFalse(propertyItty.hasNext());

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("foo");
        Assert.assertEquals(1L, vp.value());
        Assert.assertEquals(property.id(), vp.id());
    }

    @Test
    public void testVP_DateLongMismatch_PropertySet() {
        // Test that the edge case is handled correctly since dates are stored as long in Aerospike
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_PredicateMatchPriority_PropertySet")
                .property("foo", 1672531200000L).next();
        final Iterator<VertexProperty<Object>> propertyItty = v.properties();
        Assert.assertTrue(propertyItty.hasNext());
        final VertexProperty<Object> property = propertyItty.next();
        Assert.assertFalse(propertyItty.hasNext());

        final Date date = new Date(1672531200000L);
        try {
            g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", date).iterate();
            Assert.fail("Epoch time matching long value Should have triggered an exception");
        } catch (final AerospikeGraphException e) {
            if (e.errorCode != GraphError.SET_CARDINALITY_TYPE_CONFLICT.code) {
                Assert.fail("Incorrect error from set value type conflict");
            }
        }

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("foo");
        Assert.assertEquals(1672531200000L, vp.value());
        Assert.assertEquals(property.id(), vp.id());
    }

    @Test
    public void testVP_NoStartingValue_PropertySetWithVPProperties() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_PropertySetWithVPProperties").next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(0L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1, "bar", 2, "baz", "qux").iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("foo");
        Assert.assertEquals(1, vp.value());
        final long vppCount = IteratorUtils.count(vp.properties());
        Assert.assertEquals(2L, vppCount);
        Assert.assertEquals(2, vp.property("bar").value());
        Assert.assertEquals("qux", vp.property("baz").value());
    }

    @Test
    public void testVP_MismatchStartingValue_PropertySetWithVPProperties() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_PropertySetWithVPProperties").property("foo", 3).next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(1L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1, "bar", 2, "baz", "qux").iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(2L, propertyCountAfter);
        final VertexProperty vp = (VertexProperty) g.V(v.id()).properties("foo").hasValue(1).next();
        Assert.assertEquals(1, vp.value());
        final long vppCount = IteratorUtils.count(vp.properties());
        Assert.assertEquals(2L, vppCount);
        Assert.assertEquals(2, vp.property("bar").value());
        Assert.assertEquals("qux", vp.property("baz").value());
    }

    @Test
    public void testVP_SingleMatchingValueNoProperties_PropertySetWithVPProperties() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_PropertySetWithVPProperties")
                .property("foo", 1).next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(1L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1, "bar", 2, "baz", "qux").iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("foo");
        Assert.assertEquals(1, vp.value());
        final long vppCount = IteratorUtils.count(vp.properties());
        Assert.assertEquals(2L, vppCount);
        Assert.assertEquals(2, vp.property("bar").value());
        Assert.assertEquals("qux", vp.property("baz").value());
    }

    @Test
    public void testVP_SingleMatchingValueWithProperties_PropertySetWithVPProperties() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_NoStartingValue_PropertySetWithVPProperties")
                .property("foo", 1, "bar", 3, "baz", "quux", "corge", "grault").next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(1L, propertyCount);

        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1, "bar", 2, "baz", "qux").iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(1L, propertyCountAfter);
        final VertexProperty vp = vAfter.property("foo");
        Assert.assertEquals(1, vp.value());
        final long vppCount = IteratorUtils.count(vp.properties());
        Assert.assertEquals(3L, vppCount);
        Assert.assertEquals(2, vp.property("bar").value());
        Assert.assertEquals("qux", vp.property("baz").value());
        Assert.assertEquals("grault", vp.property("corge").value());
    }

    @Test
    public void testVP_MultiMatchingValues_PropertySetWithVPProperties() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_MultiMatchingValues_PropertySetWithVPProperties")
                .property("foo", 1, "bar", 2).next();
        g.V(v.id()).property(VertexProperty.Cardinality.list, "foo", 1, "bar", 2).iterate();
        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1, "bar", 3).iterate();

        final FireflyVertex vAfter = (FireflyVertex) g.V(v.id()).next();
        final long propertyCountAfter = IteratorUtils.count(vAfter.properties());
        Assert.assertEquals(2L, propertyCountAfter);
        final Iterator<VertexProperty<Object>> vps = vAfter.properties();
        boolean found2 = false;
        boolean found3 = false;
        while (vps.hasNext()) {
            final VertexProperty<Object> vp = vps.next();
            if (vp.property("bar").value().equals(2)) {
                if (found2) {
                    Assert.fail("Found VP Property with value 2 twice");
                }
                found2 = true;
            }
            if (vp.property("bar").value().equals(3)) {
                if (found3) {
                    Assert.fail("Found VP Property with value 3 twice");
                }
                found3 = true;
            }
        }
        Assert.assertTrue(found2);
        Assert.assertTrue(found3);
    }

    @Test
    public void testVP_EqualsAndPredMatchingValuesPriority_PropertySetWithVPProperties() {
        FireflyVertex v = (FireflyVertex) g.addV("testVP_EqualsAndPredMatchingValuesPriority_PropertySetWithVPProperties")
                .property("foo", 1, "meta", "int").next();
        g.V(v.id()).property(VertexProperty.Cardinality.list, "foo", 1L, "meta", "long").iterate();
        Object intId = g.V(v.id()).properties("foo").has("meta", "int").id().next();
        Object longId = g.V(v.id()).properties("foo").has("meta", "long").id().next();
        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1, "meta", "updated").iterate();

        GraphTraversal t = g.V(v.id()).properties("foo").has("meta", "updated");
        Assert.assertTrue(t.hasNext());
        Assert.assertEquals(intId, ((VertexProperty) t.next()).id());
        Assert.assertFalse(t.hasNext());
        g.V().drop().iterate();

        v = (FireflyVertex) g.addV("testVP_MultiMatchingValuesPriority_PropertySetWithVPProperties")
                .property("foo", 1L, "meta", "long").next();
        g.V(v.id()).property(VertexProperty.Cardinality.list, "foo", 1, "meta", "int").iterate();
        longId = g.V(v.id()).properties("foo").has("meta", "long").id().next();
        intId = g.V(v.id()).properties("foo").has("meta", "int").id().next();
        g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1, "meta", "updated").iterate();

        t = g.V(v.id()).properties("foo").has("meta", "updated");
        Assert.assertTrue(t.hasNext());
        Assert.assertEquals(intId, ((VertexProperty) t.next()).id());
        Assert.assertFalse(t.hasNext());
    }

    @Test
    public void testVP_PredOnlyMatchingValuesPriority_PropertySetWithVPProperties() {
        int i = 0;
        boolean longMatched = false;
        boolean doubleMatched = false;
        while (i < 1000 && !(longMatched && doubleMatched)) {
            final FireflyVertex v = (FireflyVertex) g.addV("testVP_PredOnlyMatchingValuesPriority_PropertySetWithVPProperties")
                    .property("foo", 1.0, "meta", "double").next();
            g.V(v.id()).property(VertexProperty.Cardinality.list, "foo", 1L, "meta", "long").iterate();
            final Object doubleId = g.V(v.id()).properties("foo").has("meta", "double").id().next();
            final Object longId = g.V(v.id()).properties("foo").has("meta", "long").id().next();
            g.V(v.id()).property(VertexProperty.Cardinality.set, "foo", 1, "meta", "updated").iterate();
            Assert.assertEquals(2L, IteratorUtils.count(g.V(v.id()).properties()));

            final GraphTraversal t = g.V(v.id()).properties("foo").has("meta", "updated");
            Assert.assertTrue(t.hasNext());
            final Object updatedId = ((VertexProperty) t.next()).id();
            if (doubleId.equals(updatedId)) {
                doubleMatched = true;
            } else if (longId.equals(updatedId)) {
                longMatched = true;
            } else {
                Assert.fail("Updated VP ID is not an existing VP ID");
            }
            Assert.assertFalse(t.hasNext());
            g.V().drop().iterate();
            i++;
        }
        Assert.assertTrue(longMatched);
        Assert.assertTrue(doubleMatched);
    }

    @Test
    public void testVP_ConcurrentDelete_PropertySetAddVPProperties() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVP_ConcurrentDelete_PropertySetAddVPProperties").next();
        final AtomicBoolean running = new AtomicBoolean(true);
        final Thread writeThread = new Thread(() -> {
            while (running.get()) {
                g.V(v.id()).property(VertexProperty.Cardinality.list, "foo", 1L).iterate();
            }
        });
        final Thread dropThread = new Thread(() -> {
            while (running.get()) {
                g.V(v.id()).properties("foo").drop().iterate();
            }
        });
        writeThread.start();
        dropThread.start();
        try {
            final long startTime = System.currentTimeMillis();
            while (System.currentTimeMillis() - startTime < 500) {
                g.V(v.id()).
                        property(VertexProperty.Cardinality.set, "foo", 1L, "bar", "baz").iterate();
            }
            running.set(false);
            try {
                writeThread.join();
                dropThread.join();
            } catch (final Exception e) {
                Assert.fail("Could not join writer or dropper thread");
            }
        } finally {
            running.set(false);
        }
    }

    @Test
    public void g_V_hasXname_fooX_propertyXname_setXbarX_age_43X() {
        g.addV().property(VertexProperty.Cardinality.single, "name", "foo").property("age", 42).iterate();
        final Map<Object, Object> properties = Map.of("name", VertexProperty.Cardinality.set("bar"), "age", 43);
        final List<?> verticesList = g.V().has("name", "foo").property(properties).toList();
        Assert.assertEquals(1, verticesList.size());
        Assert.assertEquals(1L, IteratorUtils.count(g.V().has("name", "foo")));
        Assert.assertEquals(1L, IteratorUtils.count(g.V().has("name", "bar")));
        Assert.assertEquals(1L, IteratorUtils.count(g.V().has("age", 43)));
        Assert.assertEquals(0L, IteratorUtils.count(g.V().has("age", 42)));
    }
}
