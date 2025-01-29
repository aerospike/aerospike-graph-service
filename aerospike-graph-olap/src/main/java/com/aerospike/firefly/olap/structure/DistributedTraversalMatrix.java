package com.aerospike.firefly.olap.structure;

import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class DistributedTraversalMatrix<S, E> {

    private final Map<String, Step<?, ?>> matrix = new HashMap<>();
    private final Map<Set<String>, Step<?, ?>> matrixByLabels = new HashMap<>();
    private final Traversal.Admin<S, E> traversal;

    public DistributedTraversalMatrix(final Traversal.Admin<S, E> traversal) {
        this.harvestSteps(this.traversal = traversal);
    }

    public <A, B, C extends Step<A, B>> C getStepById(final String stepId) {
        return (C) this.matrix.get(stepId);
    }

    public Traversal.Admin<S, E> getTraversal() {
        return this.traversal;
    }

    private final void harvestSteps(final Traversal.Admin<?, ?> traversal) {
        for (final Step<?, ?> step : traversal.getSteps()) {
            this.matrix.put(step.getId(), step);
            this.matrixByLabels.put(step.getLabels(), step);
            if (step instanceof TraversalParent) {
                for (final Traversal.Admin<?, ?> globalChild : ((TraversalParent) step).getGlobalChildren()) {
                    this.harvestSteps(globalChild);
                }
                for (final Traversal.Admin<?, ?> localChild : ((TraversalParent) step).getLocalChildren()) {
                    this.harvestSteps(localChild);
                }
            }
        }
    }

    public <A, B, C extends Step<A, B>> C getStepByLabels(final Set<String> stepId) {
        return (C) this.matrix.get(stepId);
    }
}
