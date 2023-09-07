package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.step.FireflyCompositeIdStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyCompositeEdgeIdStrategy extends FireflyStrategyBase {

    final ThreadLocal<Boolean> rootGroup = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    /**
     * Default constructor for FireflyCompositeEdgeIdStrategy.
     */
    public FireflyCompositeEdgeIdStrategy() {
    }

    @Override
    public String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_COMPOSITE_ID_STRATEGY;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // Reset whenever root.
        if (traversal.isRoot()) {
            rootGroup.set(false);
        }

        if (!traversal.isRoot()) {
            if (rootGroup.get()) {
                return;
            }
            final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();
            if (!graph.getBaseGraph().ENABLE_EMBEDDED_COMPOSITE_ID_STRATEGY) {
                return;
            }
        }

        if (TraversalHelper.onGraphComputer(traversal))
            return;
        final List<Step> steps = traversal.getSteps();

        // TODO GRAPH-792: There's a weird interaction between the strategy and traversals like:
        //  g.V().out().groupCount().by(outE().fold()).toList().
        //  With these traversals there's a casting error that occurs at the end of the traversal pipe.
        if (traversal.isRoot()) {
            for (int i = 0; i < steps.size(); i++) {
                if (steps.get(i) instanceof GroupStep || steps.get(i) instanceof GroupSideEffectStep) {
                    rootGroup.set(true);
                    break;
                }
            }
        }

        // We need to find VertexSteps.
        // In particular, we need vertex steps that return a vertex.
        for (int index = 0; index < steps.size(); index++) {
            // If it's not a VertexStep, skip it.
            if (!(steps.get(index) instanceof VertexStep)) {
                continue;
            }

            // Cast to VertexStep so we have access to the methods.
            final VertexStep<?> vertexStep = (VertexStep<?>) steps.get(index);

            // If it does not return a vertex, skip it. This is the case for something like:
            //  g.V().outE() <- In this case we can let the tinkerpop core handle it.
            if (!vertexStep.returnsVertex()) {
                continue;
            }

            // Replace vertex step with composite id step.
            traversal.removeStep(vertexStep);

            // Peek after the VertexStep to see if there is any HasStep's. If there are grab them all and push
            // them into the FireflyVertexStep.
            // Null has container is okay, it simply means that there is no HasContainer.
            // Note we don't want to push down ids.
            List<HasContainer> hasContainers = null;
            Set<String> labels = vertexStep.getLabels();

            // TODO GRAPH-402: Investigate HasContainer aware LazyBarrierStep in place of NoOpBarrierStep/HasStep.
            if ((labels.isEmpty() && index < steps.size() && steps.get(index) instanceof HasStep) ||
                    (index + 1 < steps.size() && steps.get(index) instanceof NoOpBarrierStep &&
                            steps.get(index + 1) instanceof HasStep)) {
                if (steps.get(index) instanceof NoOpBarrierStep) {
                    traversal.removeStep(steps.get(index));
                }

                final HasStep<?> hasStep = (HasStep<?>) steps.get(index);
                hasContainers = hasStep.getHasContainers();

                // Ensure there are no labels since a hasStep sometimes contains labels for the step before it.
                if (hasStep.getLabels().isEmpty() && hasContainers.stream().map(HasContainer::getKey).noneMatch(key -> key.equals(T.id.getAccessor()))) {
                    labels = hasStep.getLabels();
                    traversal.removeStep(hasStep);
                } else {
                    hasContainers = null;
                }
            }
            traversal.addStep(index, new FireflyCompositeIdStep(traversal, vertexStep.getDirection(), vertexStep.getEdgeLabels(), labels, hasContainers));
        }
    }
}
