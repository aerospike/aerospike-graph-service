package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.step.map.FireflyVertexEdgeLocalCountStep;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.LocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class FireflyVertexEdgeLocalCountStrategy extends FireflyStrategyBase {
    private static final Logger LOG = LoggerFactory.getLogger(FireflyVertexEdgeLocalCountStrategy.class);

    /**
     * Default constructor for FireflyVertexEdgeLocalCountStrategy.
     */
    public FireflyVertexEdgeLocalCountStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!traversal.isRoot()) {
            final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();
            if (!graph.getBaseGraph().ENABLE_EMBEDDED_VERTEX_EDGE_LOCAL_COUNT_STRATEGY) {
                return;
            }
        }

        if (TraversalHelper.onGraphComputer(traversal))
            return;

        for (final LocalStep localStep : TraversalHelper.getStepsOfClass(LocalStep.class, traversal)) {
            final List<Traversal.Admin> localTraversal = localStep.getLocalChildren();
            final List<Step> localTraversalSteps = localTraversal.get(0).getSteps();
            if (localTraversalSteps.size() != 2) {
                continue;
            }
            final VertexStep vertexStep;
            if (!(localTraversalSteps.get(0) instanceof VertexStep)) {
                continue;
            } else {
                vertexStep = (VertexStep) localTraversalSteps.get(0);
                if (vertexStep.getEdgeLabels().length != 0) {
                    continue;
                }
            }
            if (!(localTraversalSteps.get(1) instanceof CountGlobalStep)) {
                continue;
            }

            LOG.debug("Applying FireflyVertexEdgeLocalCountStrategy");
            TraversalHelper.replaceStep(
                    localStep,
                    new FireflyVertexEdgeLocalCountStep(traversal, vertexStep.getDirection(), localStep.getLabels()),
                    traversal);
        }

        final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();
        if (!graph.getBaseGraph().ENABLE_EMBEDDED_LOCAL_COUNT_STRATEGY) {
            return;
        }

        // Use global strategy for root. This is for embedded.
        if (traversal.isRoot()) {
            return;
        }

        // Grab steps.
        final List<Step> steps = traversal.getSteps();
        if (steps.size() < 2) {
            return;
        }

        final VertexStep vertexStep;
        final CountGlobalStep countGlobalStep;
        if (!(steps.get(0) instanceof VertexStep)) {
            return;
        } else {
            vertexStep = (VertexStep) steps.get(0);
            if (vertexStep.getEdgeLabels().length != 0) {
                return;
            }
        }
        if (!(steps.get(1) instanceof CountGlobalStep)) {
            return;
        } else {
            countGlobalStep = (CountGlobalStep) steps.get(1);
        }

        LOG.debug("Applying FireflyVertexEdgeLocalCountStrategy");
        traversal.removeStep(vertexStep);
        traversal.removeStep(countGlobalStep);

        // Grab labels.
        final Set<String> labels = new HashSet<>(vertexStep.getLabels());
        labels.addAll(countGlobalStep.getLabels());

        // Inject step.
        traversal.addStep(0, new FireflyVertexEdgeLocalCountStep(traversal, vertexStep.getDirection(), labels));
    }

}
