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

package com.aerospike.firefly.structure.id;

import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.NoSuchElementException;

public class EdgeIDTest extends AbstractFireflySuite {
    @Override
    protected boolean clearData() {
        return true;
    }

    @Test
    public void testInvalidEdgeIds() {
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
                "AAAAAAAAAAAAAAAAAAAAAAA==",
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

    @Test
    public void testNullEdgeId() {
        final GraphTraversalSource g = graph.traversal();
        String nullEdgeId = null;
        try {
            g.E(nullEdgeId).next();
            Assert.fail("Expected NoSuchElementException for id: " + nullEdgeId);
        }
        catch (NoSuchElementException e) {
            // success!
            Assert.assertTrue(true);
        }
    }
}
