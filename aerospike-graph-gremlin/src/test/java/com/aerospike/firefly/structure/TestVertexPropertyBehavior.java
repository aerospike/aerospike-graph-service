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

import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class TestVertexPropertyBehavior extends AbstractFireflySuite {

    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void verifyUpdateChangesId() {
        // Current behavior of firefly is that if you change a property value, it is effectively a new property
        // because of this, the expected behavior is that we change the id().
        final GraphTraversalSource g = graph.traversal();
        final Vertex alice29 = g.addV("person").property("name", "Alice").property("age", 29).next();
        final Vertex alice30 = g.V().has("name", "Alice").property("age", 30).next();
        assertEquals(alice29.id(), alice30.id());
        assertEquals(29, alice29.property("age").value());
        assertEquals(30, alice30.property("age").value());
        assertNotEquals(alice29.property("age").id(), alice30.property("age").id());
        final List<Vertex> vertices = g.V().has("name", "Alice").toList();
        assertEquals(1, vertices.size());
        assertEquals(alice29.id(), vertices.get(0).id());
        assertEquals(30, vertices.get(0).property("age").value());
    }

    @Test
    public void verifyVertexPropertyUseSuppliedIdsDisabled() {
        final GraphTraversalSource g = graph.traversal();
        Assert.assertThrows(UnsupportedOperationException.class, () ->
                g.addV("foo").property("name", "Alice", T.id, 199).next());
    }
}
