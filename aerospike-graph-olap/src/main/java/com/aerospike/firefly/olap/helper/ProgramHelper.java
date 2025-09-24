package com.aerospike.firefly.olap.helper;

import com.aerospike.firefly.olap.process.ConnectedComponentProgram;
import com.aerospike.firefly.olap.process.FireflyProgram;
import com.aerospike.firefly.olap.process.PageRankProgram;
import com.aerospike.firefly.olap.process.PeerPressureProgram;
import com.aerospike.firefly.olap.process.TraversalProgram;
import com.aerospike.firefly.process.computer.VertexProgramConfig;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.clustering.connected.ConnectedComponentVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.clustering.peerpressure.PeerPressureVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.TraverserGenerator;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.apache.tinkerpop.gremlin.process.computer.VertexProgram.VERTEX_PROGRAM;
import static org.apache.tinkerpop.gremlin.process.traversal.Traverser.Admin.HALT;

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
            if (config.get(VERTEX_PROGRAM).equals(PeerPressureVertexProgram.class.getName())) {
                return new PeerPressureProgram(config, graph);
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

    public static String getStartStep(final Memory memory, final String memoryKey, final Traversal traversal) {
        if (memory.exists(memoryKey)) {
            return memory.get(memoryKey);
        }
        return traversal.asAdmin().getStartStep().getId();
    }

    public static void executeVertexProgram(final TraverserSet traversers,
                                            final PureTraversal<Vertex, ?> vertexProgramTraversal,
                                            final String startStepId) {
        if (vertexProgramTraversal != null && !vertexProgramTraversal.get().getSteps().isEmpty() && !startStepId.equals(HALT)) {
            traversers.forEach(traverser -> ((Traverser.Admin) traverser).setStepId(startStepId));

            final Traversal.Admin traversal = vertexProgramTraversal.get();
            for (Object s : traversal.getSteps()) {
                if (((Step) s).getId().equals(startStepId)) {
                    if (s instanceof GraphStep) {
                        if (((GraphStep) s).getIds().length == 0) {
                            ((GraphStep) s).setIteratorSupplier(() -> traversers.stream().map(t -> ((Traverser) t).get()).iterator());
                        } else {
                            final List ids = Arrays.asList(((GraphStep) s).getIds());
                            ((GraphStep) s).setIteratorSupplier(()
                                    -> traversers.stream().map(t -> ((Traverser) t).get()).filter(t -> ids.contains(((Vertex) t).id())).iterator());
                        }
                    } else {
                        ((Step) s).addStarts(traversers.iterator());
                    }
                    break;
                }
            }

            final TraverserGenerator tg = vertexProgramTraversal.get().getTraverserGenerator();
            final TraverserSet result = new TraverserSet();
            traversal.forEachRemaining(t -> result.add(tg.generate(t, (Step) traversal.getSteps().get(0), 1)));
            traversers.clear();
            traversers.addAll(result);
        }
    }

    public static List<String> getVertexIds(final Vertex vertex, final String propertyName, final Direction direction) {
        if (vertex instanceof DetachedVertex) {
            return (List<String>) vertex.property(propertyName).value();
        }
        final List<FireflyId> cachedIds = IteratorUtils.asList(
                ((FireflyVertex) vertex).getVertexIdsFromVertex(direction, Collections.emptySet()));

        final List<String> vertexIds = new ArrayList<>(cachedIds.size());
        cachedIds.forEach(id -> vertexIds.add(id.toString()));
        return vertexIds;
    }

    public static Long getVertexIdCount(final Vertex vertex, final String propertyName, final Direction direction, final String[] edgeLabels) {
        if (vertex instanceof DetachedVertex) {
            return vertex.<Long>property(propertyName).value();
        }

        return ((FireflyVertex) vertex).getEdgeCount(direction, edgeLabels);
    }
}
