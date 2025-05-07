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

        Object[] invalidIds = new Object[] {
                // invalid char
                "VXNlci1J:RDoxMjM0NTY3OA==",
                // integer
                "1287563",
                // double
                "123.456",
                // long
                "1234567890123456789L",
                // float
                "3.14f",
                // empty string
                "",
                // valid but too long
                "AAAAAAAAAAAAAAAAAAAAAA==",
                // valid but too short
                "AA=="
        };

        for (Object id : invalidIds) {
            try {
                g.E(id).next();
                Assert.fail("Expected IllegalArgumentException for id: " + id);
            }
            catch (IllegalArgumentException e) {
                String repr = String.valueOf(id);   // turns null → "null"
                String expected = "Invalid id for edge: '" + repr +
                        "'. Base64 encoded String did not decode to a valid 8 or 16 byte array.";
                Assert.assertTrue(
                        "For id=\"" + repr + "\" expected message to contain:\n  " + expected +
                                "\nbut was:\n  " + e.getMessage(),
                        e.getMessage().contains(expected)
                );
            }
        }
    }

}
