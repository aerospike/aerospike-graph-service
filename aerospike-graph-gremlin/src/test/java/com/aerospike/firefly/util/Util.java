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

package com.aerospike.firefly.util;

import com.aerospike.firefly.structure.FireflyGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;

public class Util {
    private static Logger LOG = LoggerFactory.getLogger(Util.class);

    public static void verifyClean(final FireflyGraph graph) {
        final long vertexCount = graph.traversal().V().count().next();
        if (vertexCount > 0) {
            final var vertices = graph.traversal().V();
            while (true) {
                try {
                    final var vertex = vertices.next();
                    LOG.error("Found non-dropped vertex with ID '" + vertex.id() + "' and label '" + vertex.label() + "'");
                } catch (final NoSuchElementException e) {
                    LOG.error("Done listing non-dropped vertices");
                    break;
                }
            }
            throw new RuntimeException("Non-zero vertex count after drop operation: " + vertexCount);
        }
        final long edgeCount = graph.traversal().E().count().next();
        if (edgeCount > 0) {
            final var edges = graph.traversal().E();
            while (true) {
                try {
                    final var edge = edges.next();
                    LOG.error("Found non-dropped edge with ID '" + edge.id() + "' and label '" + edge.label() + "'");
                } catch (final NoSuchElementException e) {
                    LOG.error("Done listing non-dropped edges");
                    break;
                }
            }
            throw new RuntimeException("Non-zero edge count after drop operation: " + edgeCount);
        }
    }

    public static void cleanAndVerifyGraph(final FireflyGraph graph) {
        graph.traversal().V().drop().iterate();
        verifyClean(graph);
    }
}
