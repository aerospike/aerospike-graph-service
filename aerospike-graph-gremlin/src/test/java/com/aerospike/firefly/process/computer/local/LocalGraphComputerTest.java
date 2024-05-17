package com.aerospike.firefly.process.computer.local;

import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyGraphFilterStrategy;
import com.aerospike.firefly.util.AbstractFireflySuite;
import org.apache.tinkerpop.gremlin.process.computer.GraphFilter;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.optimization.GraphFilterStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class LocalGraphComputerTest extends AbstractFireflySuite {

    private static final Logger LOG = LoggerFactory.getLogger(LocalGraphComputerTest.class);

    @Override
    protected boolean clearData() {
        graph.getBaseGraph().dropDatabase(graph, true);
        assertEquals(0L, graph.getVertexCount(List.of()));
        assertEquals(0L, graph.getEdgeCount());
        return true;
    }

    public void verifyGraphFilter(final GraphTraversal<?, ?> traversal, final Traversal<?, ?> vertexFilter, final Traversal<?, ?> edgeFilter) {
        assertTrue(traversal.asAdmin().getStrategies().getStrategy(FireflyGraphFilterStrategy.class).isPresent());
        assertFalse(traversal.asAdmin().getStrategies().getStrategy(GraphFilterStrategy.class).isPresent());
        traversal.asAdmin().applyStrategies();
        final GraphFilter filter = new GraphFilter(((TraversalVertexProgramStep) traversal.asAdmin().getStartStep()).getComputer());
        assertEquals(filter.hasVertexFilter(), vertexFilter != null);
        assertEquals(filter.hasEdgeFilter(), edgeFilter != null);
        if (null != vertexFilter) {
            for (int i = 0; i < filter.getVertexFilter().getSteps().size(); i++) {
                final Step<?, ?> aStep = filter.getVertexFilter().getSteps().get(i);
                final Step<?, ?> bStep = vertexFilter.asAdmin().getSteps().get(i);
                assertEquals(aStep, bStep);
            }
        }
        if (null != edgeFilter) {
            for (int i = 0; i < filter.getEdgeFilter().getSteps().size(); i++) {
                final Step<?, ?> aStep = filter.getEdgeFilter().getSteps().get(i);
                final Step<?, ?> bStep = edgeFilter.asAdmin().getSteps().get(i);
                assertEquals(aStep, bStep);
            }
        }
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    @Test
    public void testGraphFilterConstruction() {
        final Object NONE = new Object() {
            @Override
            public String toString() {
                return "NONE";
            }
        };
        final GraphTraversalSource g = graph.traversal().withComputer();
        List.of(
                List.of(g.V(), NONE, __.bothE().limit(0)),
                List.of(g.V().out(), NONE, __.outE()),
                List.of(g.V().out("knows"), NONE, __.outE("knows")),
                List.of(g.V().hasLabel("person").inE("knows").outV().hasLabel("person"), NONE, __.inE("knows")), // TODO: reason person vertex filter
                List.of(g.V().hasLabel("person").in("knows").hasLabel("person"), NONE, __.inE("knows")),
                List.of(g.V().hasLabel("person"), __.hasLabel("person"), __.bothE().limit(0)),
                List.of(g.V().has("name", "marko"), __.has("name", "marko"), __.bothE().limit(0)),
                List.of(g.V().has("name", "marko").has("age", 32), __.has("name", "marko").has("age", 32), __.bothE().limit(0)),
                List.of(g.V().has("name", "marko").has("age", 32).out("knows", "likes"), NONE, __.outE("knows", "likes")),
                List.of(g.V().hasLabel("person").out("knows").hasLabel("person"), NONE, __.outE("knows")),// TODO: reason person vertex filter
                List.of(g.V().outE("knows").inV(), NONE, __.outE("knows")),
                List.of(g.V().outE("knows").inV().out("knows"), NONE, __.outE("knows")),
                List.of(g.V().outE("knows").inV().out("knows").out("knows"), NONE, __.outE("knows")),
                List.of(g.V().outE("knows").inV().out("knows").in("knows"), NONE, __.bothE("knows")),
                List.of(g.V().outE("knows").inV().out("knows").in("likes"), NONE, __.union(__.inE("likes"), __.outE("knows"))),
                List.of(g.V().outE("knows").inV().in("knows").in("likes"), NONE, __.union(__.inE("likes"), __.bothE("knows")))
        ).forEach(params -> {
            LOG.warn("Testing {} with\n\tvertex filter:{}\n\tedge filter: {}", params.get(0), params.get(1), params.get(2));
            verifyGraphFilter(
                    (GraphTraversal<?, ?>) params.get(0),
                    (Traversal<?, ?>) (params.get(1) == NONE ? null : params.get(1)),
                    (Traversal<?, ?>) (params.get(2) == NONE ? null : params.get(2)));
        });

    }

    @Test
    public void testVerificationHandling() {
        Arrays.asList(
                graph.traversal().withComputer().V().pageRank(),
                graph.traversal().withComputer().V().shortestPath(),
                graph.traversal().withComputer().V().out().peerPressure().valueMap()).forEach(t -> {
            try {
                LOG.warn("Verifying graph algorithm error handling:\n\t" + t.toString());
                t.next();
                assertFalse("Should have thrown verification exception message", true);
            } catch (final VerificationException e) {
                assertTrue(true);
            }
        });
    }
}
