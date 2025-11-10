package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

public class FireflyHasIdVertexStep<S> extends AbstractStep<S, S> {
    private final Direction direction;
    private final Set<String> edgeLabels;
    private final Set<Object> targetIds;

    public FireflyHasIdVertexStep(final Traversal.Admin<?, ?> traversal,
                                  final Direction direction,
                                  final String[] edgeLabels,
                                  final List<HasContainer> hasContainers) {
        super(traversal);
        this.direction = direction;
        this.edgeLabels = new HashSet<>(Arrays.asList(edgeLabels));
        this.targetIds = extractTargetIds(hasContainers);
    }

    private static Set<Object> extractTargetIds(final List<HasContainer> hasContainers) {
        final Set<Object> ids = new HashSet<>();
        for (HasContainer hc : hasContainers) {
            if (hc.getKey().equals(T.id.getAccessor())) {
                ids.add(hc.getValue());
            }
        }
        return ids;
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        while (true) {
            final Traverser.Admin<S> traverser = this.starts.next();
            final S element = traverser.get();

            final FireflyVertex vertex;
            if (element instanceof FireflyVertex) {
                vertex = (FireflyVertex) element;
            } else if (element instanceof ComputerGraph.ComputerVertex) {
                vertex = (FireflyVertex) ((ComputerGraph.ComputerVertex) element).getBaseVertex();
            } else {
                continue; // skip non-vertex traversers
            }

            final Iterator<FireflyId> neighborIds =
                    vertex.getVertexIdsFromVertex(direction, edgeLabels);
            while (neighborIds.hasNext()) {
                final FireflyId neighborId = neighborIds.next();
                if (targetIds.contains(neighborId.getUserId())) {
                    return traverser;
                }
            }
            // No matches, move to next traverser
        }
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
