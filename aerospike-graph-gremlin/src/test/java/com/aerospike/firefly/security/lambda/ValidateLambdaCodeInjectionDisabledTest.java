package com.aerospike.firefly.security.lambda;

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

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
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
