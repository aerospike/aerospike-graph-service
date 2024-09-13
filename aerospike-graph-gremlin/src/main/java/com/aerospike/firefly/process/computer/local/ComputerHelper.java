package com.aerospike.firefly.process.computer.local;

import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;

public class ComputerHelper {
    public static boolean isComputerTraversal(final Traversal.Admin<?, ?> traversal) {
        Traversal.Admin<?, ?> t = traversal;
        while (!(t.getParent() instanceof EmptyStep)) {
            t = t.getParent().asStep().getTraversal();
        }
        if (!t.getSteps().isEmpty()) {
            return t.getSteps().get(0) instanceof TraversalVertexProgramStep;
        }
        return false;
    }
}
