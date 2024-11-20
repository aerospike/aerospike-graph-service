package com.aerospike.firefly.process.computer.local;

import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

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
}
