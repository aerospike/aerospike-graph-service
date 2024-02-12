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
        final Vertex lyndon29 = g.addV("person").property("name", "Lyndon").property("age", 29).next();
        final Vertex lyndon30 = g.V().has("name", "Lyndon").property("age", 30).next();
        assertEquals(lyndon29.id(), lyndon30.id());
        assertEquals(29, lyndon29.property("age").value());
        assertEquals(30, lyndon30.property("age").value());
        assertNotEquals(lyndon29.property("age").id(), lyndon30.property("age").id());
        final List<Vertex> vertices = g.V().has("name", "Lyndon").toList();
        assertEquals(1, vertices.size());
        assertEquals(lyndon29.id(), vertices.get(0).id());
        assertEquals(30, vertices.get(0).property("age").value());
    }

    @Test
    public void verifyVertexPropertyUseSuppliedIdsDisabled() {
        final GraphTraversalSource g = graph.traversal();
        Assert.assertThrows(UnsupportedOperationException.class, () ->
                g.addV("foo").property("name", "Lyndon", T.id, 199).next());
    }
}
