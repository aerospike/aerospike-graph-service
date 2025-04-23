package com.aerospike.firefly.olap.process;

import com.aerospike.firefly.process.computer.VertexProgramConfig;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.clustering.connected.ConnectedComponentVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.B_O_TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.computer.VertexProgram.VERTEX_PROGRAM;

public class ProgramHelper {
    private ProgramHelper() {
    }

    public static FireflyProgram createVertexProgram(final Configuration vertexProgramConfig, final FireflyGraph graph) {
        final VertexProgram vp = VertexProgram.createVertexProgram(graph, vertexProgramConfig);
        return createVertexProgram(vp, graph);
    }

    public static FireflyProgram createVertexProgram(final VertexProgram vertexProgram, final FireflyGraph graph) {
        if (vertexProgram instanceof FireflyProgram) {
            return (FireflyProgram) vertexProgram;
        }

        if (vertexProgram instanceof TraversalVertexProgram) {
            return new TraversalProgram((TraversalVertexProgram) vertexProgram);
        }

        if (vertexProgram instanceof VertexProgramConfig) {
            final VertexProgramConfig config = (VertexProgramConfig) vertexProgram;

            if (config.get(VERTEX_PROGRAM).equals(PageRankVertexProgram.class.getName())) {
                return new PageRankProgram(config, graph);
            }
            if (config.get(VERTEX_PROGRAM).equals(ConnectedComponentVertexProgram.class.getName())) {
                return new ConnectedComponentProgram(config, graph);
            }

            throw new IllegalArgumentException("Unsupported program " + config.get(VERTEX_PROGRAM) + ", please contact support.");
        }

        throw new IllegalArgumentException("Unsupported program type " + vertexProgram.getClass().getName() + ", please contact support.");
    }

    public static void removeTemporaryProperties(final TraverserSet traversers, final String propertyToKeep) {
        for (final Object traverser : traversers) {
            final DetachedVertex v = (DetachedVertex) ((Traverser) traverser).get();
            final List<VertexProperty> properties = new ArrayList<>();
            final Iterator<VertexProperty<Object>> itty = v.properties();
            while (itty.hasNext()) {
                final VertexProperty<Object> p = itty.next();
                if (p.key().equals(propertyToKeep)) {
                    properties.add(p);
                    break;
                }
            }
            ((Traverser) traverser).asAdmin().set(new DetachedVertex(v.id(), v.label(), properties));
        }
    }

    public static void executeVertexProgram(final TraverserSet traversers, final PureTraversal<Vertex, ?> vertexProgramTraversal) {
        if (vertexProgramTraversal != null && !vertexProgramTraversal.get().getSteps().isEmpty()) {
            final Traversal.Admin traversal = vertexProgramTraversal.get();
            if (traversal.getSteps().get(0) instanceof GraphStep) {
                final GraphStep graphStep = (GraphStep) traversal.getSteps().get(0);
                if (graphStep.getIds().length == 0) {
                    graphStep.setIteratorSupplier(
                            () -> traversers.stream().map(t -> ((Traverser) t).get()).iterator());
                } else {
                    final List ids = Arrays.asList(graphStep.getIds());
                    final TraverserSet finalTraversers = traversers;
                    graphStep.setIteratorSupplier(
                            () -> finalTraversers.stream().map(t -> ((Traverser) t).get()).filter(v -> ids.contains(((Vertex) v).id())).iterator());
                }
            }
            final TraverserGenerator tg = vertexProgramTraversal.get().getTraverserGenerator();
            final TraverserSet result = new TraverserSet();
            traversal.forEachRemaining(t -> result.add(tg.generate(t, (Step) traversal.getSteps().get(0), 1)));
            traversers.clear();
            traversers.addAll(result);
        }
    }
}
