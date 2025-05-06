package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author Connor Hengstler (<a href="https://github.com/OblivionBC">https://github.com/OblivionBC</a>)
 */
public class EdgeIDTest extends AbstractFireflySuite {
    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void testInvalidCharInEdgeId() {
        final GraphTraversalSource g = graph.traversal();
        final Vertex foo = g.addV("foo").next();
        final Vertex bar = g.addV("bar").next();
        final Edge baz = g.addE("baz").from(foo).to(bar).next();

        IllegalArgumentException ex = Assert.assertThrows(
                IllegalArgumentException.class,
                // lambda must be a void‐returning block or expression
                () -> g.E("VXNlci1J:RDoxMjM0NTY3OA==").next());

        Assert.assertTrue(
                "Error should be thrown as 'Invalid id for edge: 'VXNlci1J:RDoxMjM0NTY3OA=='. Base64 encoded String did not decode to a valid 8 or 16 byte array', was: " + ex.getMessage(),
                ex.getMessage().contains("Invalid id for edge: 'VXNlci1J:RDoxMjM0NTY3OA=='. Base64 encoded String did not decode to a valid 8 or 16 byte array"));
    }
}
