package com.aerospike.firefly.process.computer.local;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ComputerHelper {
    public static boolean onGraphComputer(Traversal.Admin<?, ?> traversal) {
        while (!(traversal.isRoot())) {
            if (traversal.getParent() instanceof TraversalVertexProgramStep)
                return true;
            traversal = traversal.getParent().asStep().getTraversal();
        }
        if (traversal.getSteps().size() > 0) {
            return traversal.getSteps().get(0) instanceof TraversalVertexProgramStep;
        } else {
            return false;
        }
    }

    public static void bulkAttach(final FireflyGraph graph,
                                  final TraversalSideEffects traversalSideEffects,
                                  final TraverserSet<Object> traversers) {
        final Set<Object> ids = new HashSet<>();
        traversers.forEach(traverser -> {
            if (traverser.get() instanceof Vertex)
                ids.add(((Vertex) traverser.get()).id());
        });

        final Map<Object, Element> cache = new HashMap<>();
        graph.vertices(ids.toArray(new Object[ids.size()])).forEachRemaining(vertex -> cache.put(vertex.id(), vertex));

        traversers.forEach(traverser -> {
            if (traverser.get() instanceof Vertex && cache.containsKey(((Vertex) traverser.get()).id())) {
                final Vertex vertex = (Vertex) cache.get(((Vertex) traverser.get()).id());
                traverser.attach(Attachable.Method.get(vertex));
                traverser.setSideEffects(traversalSideEffects);
            }
        });
    }
}
