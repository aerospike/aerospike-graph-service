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
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.aerospike.firefly.Tokens.INTEGRATION_TEST_PROPERTIES;

public class TestVertexPropertyPropertyCardinality {
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
    public void testVPP_SingleStartingValue_SingleMetaProperty() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVPP_SingleStartingValue_SingleMetaProperty")
                .property(VertexProperty.Cardinality.single, "name", "Bob", "age", 30)
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(1L, propertyCount);
        final Iterator<Property<Object>> vpps = v.property("name").properties();
        Assert.assertTrue(vpps.hasNext());
        final Property<Object> vp = vpps.next();
        Assert.assertEquals("age", vp.key());
        Assert.assertEquals(30, vp.value());

        final List<? extends Property<Object>> vpps2 = g.V().hasLabel("testVPP_SingleStartingValue_SingleMetaProperty").
                has("name", "Bob").properties("name").properties("age").toList();
        Assert.assertEquals(1, vpps2.size());
        Assert.assertEquals("age", vpps2.get(0).key());
        Assert.assertEquals(30, vpps2.get(0).value());
    }

    @Test
    public void testVPP_SingleStartingValue_DoubleMetaProperty() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVPP_SingleStartingValue_DoubleMetaProperty")
                .property(VertexProperty.Cardinality.single, "name", "Bob", "age", 30, "city", "London")
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(1L, propertyCount);
        final Iterator<Property<Object>> vpps = v.property("name").properties();
        Assert.assertTrue(vpps.hasNext());
        final Property<Object> vp = vpps.next();
        Assert.assertEquals("age", vp.key());
        Assert.assertEquals(30, vp.value());
        Assert.assertTrue(vpps.hasNext());
        final Property<Object> vp2 = vpps.next();
        Assert.assertEquals("city", vp2.key());
        Assert.assertEquals("London", vp2.value());

        final List<Map<Object, Object>> vpps2 = g.V().hasLabel("testVPP_SingleStartingValue_DoubleMetaProperty").
                has("name", "Bob").properties("name").valueMap().toList();
        Assert.assertEquals(1, vpps2.size());
        Assert.assertTrue(vpps2.get(0).containsKey("age"));
        Assert.assertEquals(30, vpps2.get(0).get("age"));
        Assert.assertTrue(vpps2.get(0).containsKey("city"));
        Assert.assertEquals("London", vpps2.get(0).get("city"));
    }

    @Test
    public void testVPP_DuplicateStartingValue_DoubleMetaProperty() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVPP_DuplicateStartingValue_DoubleMetaProperty")
                .property(VertexProperty.Cardinality.list, "name", "Bob", "age", 30)
                .property(VertexProperty.Cardinality.list, "name", "Bob", "city", "London")
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(2L, propertyCount);
        final Iterator<VertexProperty<Object>> vps = v.properties("name");
        final List<VertexProperty<Object>> vppsList = new ArrayList<>();
        while (vps.hasNext()) {
            vppsList.add(vps.next());
        }
        Assert.assertEquals(2, vppsList.size());
        final Iterator<Property<Object>> vpProperties = vppsList.get(0).properties();
        final Iterator<Property<Object>> vpProperties2 = vppsList.get(1).properties();
        final Iterator<Property<Object>> vpps = IteratorUtils.concat(vpProperties, vpProperties2);
        Assert.assertTrue(vpps.hasNext());
        final Property<Object> vp = vpps.next();
        if (vp.key().equals("age")) {
            Assert.assertEquals("age", vp.key());
            Assert.assertEquals(30, vp.value());
        } else {
            Assert.assertEquals("city", vp.key());
            Assert.assertEquals("London", vp.value());
        }
        Assert.assertTrue(vpps.hasNext());
        final Property<Object> vp2 = vpps.next();
        if (vp2.key().equals("age")) {
            Assert.assertEquals("age", vp2.key());
            Assert.assertEquals(30, vp2.value());
        } else {
            Assert.assertEquals("city", vp2.key());
            Assert.assertEquals("London", vp2.value());
        }

        final List<? extends Property<Object>> vpps2 = g.V().hasLabel("testVPP_DuplicateStartingValue_DoubleMetaProperty").
                has("name", "Bob").properties("name").properties().toList();
        Assert.assertEquals(2, vpps2.size());
        Assert.assertTrue(vpps2.stream().anyMatch(p -> p.key().equals("age") && p.value().equals(30)));
        Assert.assertTrue(vpps2.stream().anyMatch(p -> p.key().equals("city") && p.value().equals("London")));
    }

    @Test
    public void testVPP_DoubleStartingValue_DoubleMetaProperty() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVPP_DoubleStartingValue_DoubleMetaProperty")
                .property(VertexProperty.Cardinality.list, "name", "Bob", "age", 30, "city", "London")
                .property(VertexProperty.Cardinality.list, "name", "Bob")
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(2L, propertyCount);
        final List<? extends Property<Object>> vpps = g.V(v.id()).properties("name").properties().toList();
        Assert.assertEquals(2, vpps.size());
        for (final Property<Object> vp : vpps) {
            if (vp.key().equals("age")) {
                Assert.assertEquals("age", vp.key());
                Assert.assertEquals(30, vp.value());
            } else if (vp.key().equals("city")) {
                Assert.assertEquals("city", vp.key());
                Assert.assertEquals("London", vp.value());
            } else {
                Assert.fail("Unexpected property key: " + vp.key());
            }
        }

        final List<Map<Object, Object>> vpps2 = g.V().hasLabel("testVPP_DoubleStartingValue_DoubleMetaProperty").
                has("name", "Bob").properties("name").valueMap().toList();
        Assert.assertEquals(2, vpps2.size());
        final Map<Object, Object> map = vpps2.get(0).isEmpty() ? vpps2.get(1) : vpps2.get(0);
        Assert.assertTrue(map.containsKey("age"));
        Assert.assertEquals(30, map.get("age"));
        Assert.assertTrue(map.containsKey("city"));
        Assert.assertEquals("London", map.get("city"));
    }

    @Test
    public void testVPP_SingleStartingValue_SingleMetaPropertyRemove() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVPP_SingleStartingValue_SingleMetaPropertyRemove")
                .property(VertexProperty.Cardinality.single, "name", "Bob", "age", 30)
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(1L, propertyCount);
        final Iterator<Property<Object>> vpps = v.property("name").properties();
        Assert.assertTrue(vpps.hasNext());
        final Property<Object> vp = vpps.next();
        Assert.assertEquals("age", vp.key());
        Assert.assertEquals(30, vp.value());

        // Remove the meta-property
        g.V().hasLabel("testVPP_SingleStartingValue_SingleMetaPropertyRemove").
                properties("name").properties("age").drop().iterate();

        // Verify the property is removed
        final List<? extends Property<Object>> vpps2 = g.V().hasLabel("testVPP_SingleStartingValue_SingleMetaPropertyRemove")
                .properties("name").properties().toList();
        Assert.assertEquals(0, vpps2.size());
    }

    @Test
    public void testVPP_SingleStartingValue_DoubleMetaPropertySingleRemove() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVPP_SingleStartingValue_DoubleMetaPropertyRemove")
                .property(VertexProperty.Cardinality.single, "name", "Bob", "age", 30, "city", "London")
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(1L, propertyCount);
        final Iterator<Property<Object>> vpps = v.property("name").properties();
        Assert.assertTrue(vpps.hasNext());
        final Property<Object> vp = vpps.next();
        if (vp.key().equals("age")) {
            Assert.assertEquals("age", vp.key());
            Assert.assertEquals(30, vp.value());
        } else {
            Assert.assertEquals("city", vp.key());
            Assert.assertEquals("London", vp.value());
        }
        Assert.assertTrue(vpps.hasNext());
        final Property<Object> vp2 = vpps.next();
        if (vp2.key().equals("age")) {
            Assert.assertEquals("age", vp2.key());
            Assert.assertEquals(30, vp2.value());
        } else {
            Assert.assertEquals("city", vp2.key());
            Assert.assertEquals("London", vp2.value());
        }

        // Remove the meta-properties
        g.V().hasLabel("testVPP_SingleStartingValue_DoubleMetaPropertyRemove").
                properties("name").properties("age").drop().iterate();

        // Verify the property is removed
        final List<? extends Property<Object>> vpps2 = g.V().hasLabel("testVPP_SingleStartingValue_DoubleMetaPropertyRemove")
                .properties("name").properties().toList();
        Assert.assertEquals(1, vpps2.size());
        Assert.assertTrue(vpps2.stream().anyMatch(p -> p.key().equals("city") && p.value().equals("London")));
        Assert.assertFalse(vpps2.stream().anyMatch(p -> p.key().equals("age")));

        // Remove the remaining meta-property
        g.V().hasLabel("testVPP_SingleStartingValue_DoubleMetaPropertyRemove").
                properties("name").properties("city").drop().iterate();
        // Verify the property is removed
        final List<? extends Property<Object>> vpps3 = g.V().hasLabel("testVPP_SingleStartingValue_DoubleMetaPropertyRemove")
                .properties("name").properties().toList();
        Assert.assertEquals(0, vpps3.size());
    }

    @Test
    public void testVPP_SingleStartingValue_DoubleMetaPropertyDoubleRemove() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVPP_SingleStartingValue_DoubleMetaPropertyDoubleRemove")
                .property(VertexProperty.Cardinality.single, "name", "Bob", "age", 30, "city", "London")
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(1L, propertyCount);
        final Iterator<Property<Object>> vpps = v.property("name").properties();
        Assert.assertTrue(vpps.hasNext());
        final Property<Object> vp = vpps.next();
        if (vp.key().equals("age")) {
            Assert.assertEquals("age", vp.key());
            Assert.assertEquals(30, vp.value());
        } else {
            Assert.assertEquals("city", vp.key());
            Assert.assertEquals("London", vp.value());
        }
        Assert.assertTrue(vpps.hasNext());
        final Property<Object> vp2 = vpps.next();
        if (vp2.key().equals("age")) {
            Assert.assertEquals("age", vp2.key());
            Assert.assertEquals(30, vp2.value());
        } else {
            Assert.assertEquals("city", vp2.key());
            Assert.assertEquals("London", vp2.value());
        }

        // Remove the meta-properties
        g.V().hasLabel("testVPP_SingleStartingValue_DoubleMetaPropertyDoubleRemove").
                properties("name").properties().drop().iterate();

        // Verify the property is removed
        final List<? extends Property<Object>> vpps2 = g.V().hasLabel("testVPP_SingleStartingValue_DoubleMetaPropertyDoubleRemove")
                .properties("name").properties().toList();
        Assert.assertEquals(0, vpps2.size());
    }

    @Test
    public void testVPP_DoubleStartingValue_SingleMetaPropertySingleRemove() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVPP_DoubleStartingValue_SingleMetaPropertySingleRemove")
                .property(VertexProperty.Cardinality.list, "name", "Bob", "age", 30)
                .property(VertexProperty.Cardinality.list, "name", "Bob", "city", "London")
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(2L, propertyCount);
        final List<? extends Property<Object>> vpp = g.V().hasLabel("testVPP_DoubleStartingValue_SingleMetaPropertySingleRemove")
                .properties("name").properties().toList();
        Assert.assertEquals(2, vpp.size());
        Assert.assertTrue(vpp.stream().anyMatch(p -> p.key().equals("age") && p.value().equals(30)));
        Assert.assertTrue(vpp.stream().anyMatch(p -> p.key().equals("city") && p.value().equals("London")));

        // Remove the meta-property
        g.V().hasLabel("testVPP_DoubleStartingValue_SingleMetaPropertySingleRemove")
                .properties("name").properties("age").drop().iterate();
        // Verify the property is removed
        final List<? extends Property<Object>> vpps2 = g.V().hasLabel("testVPP_DoubleStartingValue_SingleMetaPropertySingleRemove")
                .properties("name").properties().toList();
        Assert.assertEquals(1, vpps2.size());
        Assert.assertTrue(vpps2.stream().anyMatch(p -> p.key().equals("city") && p.value().equals("London")));
        Assert.assertFalse(vpps2.stream().anyMatch(p -> p.key().equals("age")));

        // Remove the remaining meta-property
        g.V().hasLabel("testVPP_DoubleStartingValue_SingleMetaPropertySingleRemove")
                .properties("name").properties("city").drop().iterate();
        // Verify the property is removed
        final List<? extends Property<Object>> vpps3 = g.V().hasLabel("testVPP_DoubleStartingValue_SingleMetaPropertySingleRemove")
                .properties("name").properties().toList();
        Assert.assertEquals(0, vpps3.size());
    }

    @Test
    public void testVPP_DoubleStartingValue_SingleMetaPropertyDoubleRemove() {
        final FireflyVertex v = (FireflyVertex) g.addV("testVPP_SingleStartingValue_DoubleMetaPropertyDoubleRemove")
                .property(VertexProperty.Cardinality.list, "name", "Bob", "age", 30)
                .property(VertexProperty.Cardinality.list, "name", "Bob", "city", "London")
                .next();
        final long propertyCount = IteratorUtils.count(v.properties());
        Assert.assertEquals(2L, propertyCount);
        final List<? extends Property<Object>> vpp = g.V().hasLabel("testVPP_SingleStartingValue_DoubleMetaPropertyDoubleRemove")
                .properties("name").properties().toList();
        Assert.assertEquals(2, vpp.size());
        Assert.assertTrue(vpp.stream().anyMatch(p -> p.key().equals("age") && p.value().equals(30)));
        Assert.assertTrue(vpp.stream().anyMatch(p -> p.key().equals("city") && p.value().equals("London")));

        // Remove the meta-property
        g.V().hasLabel("testVPP_SingleStartingValue_DoubleMetaPropertyDoubleRemove")
                .properties("name").properties().drop().iterate();
        // Verify the property is removed
        final List<? extends Property<Object>> vpps2 = g.V().hasLabel("testVPP_SingleStartingValue_DoubleMetaPropertyDoubleRemove")
                .properties("name").properties().toList();
        Assert.assertEquals(0, vpps2.size());
    }
}
