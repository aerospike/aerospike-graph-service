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

package com.aerospike.firefly.process.strategy;

import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.GraphHelper;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerFactory;
import org.apache.tinkerpop.gremlin.util.function.Lambda;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class ValidateLambdaCodeInjectionDisabledTest extends AbstractFireflySuite {

    @Test
    public void testValidateLambdaCodeInjectionDisabled() {
        // Create graph.
        GraphHelper.cloneElements(TinkerFactory.createModern(), graph);
        final GraphTraversalSource g = graph.traversal();

        final String lambda1 = "it.get().label().equals('person')";
        try {
            final List<Vertex> markos = g.V().has("name", "marko").filter(Lambda.predicate(lambda1)).toList();
            fail("Lambda code injection should be disabled.");
        } catch (final Exception e) {
            // Expect an exception
            assertEquals("The provided traversal contains a lambda step: LambdaFilterStep(lambda[" + lambda1 + "])", e.getMessage());
        }

        final String lambda2 = "it.get().label().equals('person')";
        try {
            final List<Object> markoOuts = g.V().has("name", "marko").flatMap(Lambda.function(lambda2, "gremlin-groovy")).toList();
            fail("Lambda code injection should be disabled.");
        } catch (final Exception e) {
            // Expect an exception
            assertEquals("The provided traversal contains a lambda step: LambdaFlatMapStep(lambda[" + lambda2 + "])", e.getMessage());
        }

    }

    @Override
    protected boolean clearData() {
        return true;
    }
}
