package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.step.computer.FireflyCompositeIdStepLocal;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyCompositeEdgeIdLocalStrategy extends FireflyStrategyBase {

    final ThreadLocal<Boolean> rootGroup = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    /**
     * Default constructor for FireflyCompositeEdgeIdStrategy.
     */
    public FireflyCompositeEdgeIdLocalStrategy() {
    }

    @Override
    public String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_COMPOSITE_ID_STRATEGY;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();

        if (!TraversalHelper.onGraphComputer(traversal))
            return;

        // Reset whenever root.
        if (traversal.isRoot()) {
            rootGroup.set(false);
        }

        if (!traversal.isRoot()) {
            if (rootGroup.get()) {
                return;
            }
            if (!graph.getBaseGraph().ENABLE_EMBEDDED_COMPOSITE_ID_STRATEGY) {
                return;
            }
        }

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

            boolean propertyRemovalValid = !(super.steps.contains(PathStep.class) || super.steps.contains(TreeStep.class) || super.steps.contains(TreeSideEffectStep.class));

            // Replace vertex step with composite id step.
            traversal.removeStep(vertexStep);

            // Peek after the VertexStep to see if there is any HasStep's. If there are grab them all and push
            // them into the FireflyVertexStep.
            // Null has container is okay, it simply means that there is no HasContainer.
            // Note we don't want to push down ids.
            List<HasContainer> hasContainers = null;
            Set<String> labels = vertexStep.getLabels();
            List<String> propertyKeys = null;
            while (labels.isEmpty()) {
                if (index >= steps.size()) {
                    break;
                }
                if (steps.get(index) instanceof NoOpBarrierStep) {
                    // Grab any labels and remove the barrier.
                    final NoOpBarrierStep<?> noOpBarrierStep = (NoOpBarrierStep<?>) steps.get(index);
                    labels = noOpBarrierStep.getLabels();
                    traversal.removeStep(steps.get(index));
                } else if (steps.get(index) instanceof PropertiesStep){
                    if (traversal.isRoot() && propertyRemovalValid) {
                        // Grab any labels and remove the properties step.
                        final PropertiesStep<?> propertiesStep = (PropertiesStep<?>) steps.get(index);
                        final String[] propertyKeyArray = propertiesStep.getPropertyKeys();
                        if (propertyKeyArray == null || propertyKeyArray.length == 0) {
                            break;
                        }
                        propertyKeys = new ArrayList<>();
                        for (final String propertyKey : propertyKeyArray) {
                            if (!propertyKeys.contains(propertyKey)) {
                                propertyKeys.add(propertyKey);
                            }
                        }
                        labels = propertiesStep.getLabels();
                    }
                    break;
                } else if (steps.get(index) instanceof IdStep) {
                    if (traversal.isRoot() && propertyRemovalValid) {
                        // Grab any labels.
                        propertyKeys = new ArrayList<>();
                        final IdStep<?> idStep = (IdStep<?>) steps.get(index);
                        labels = idStep.getLabels();
                    }
                    break;
                } else if (steps.get(index) instanceof HasStep) {
                    // Grab has containers and push them down.
                    final HasStep<?> hasStep = (HasStep<?>) steps.get(index);
                    hasContainers = hasStep.getHasContainers();

                    if (steps.size() > (index + 1) && steps.get(index + 1) instanceof VertexStep) {
                        if (traversal.isRoot() && propertyRemovalValid) {
                            propertyKeys = new ArrayList<>();
                            final List<String> properties = hasContainers.stream().
                                    map(HasContainer::getKey).collect(Collectors.toList());
                            for (final String propertyKey : properties) {
                                if (!propertyKeys.contains(propertyKey)) {
                                    propertyKeys.add(propertyKey);
                                }
                            }
                        }
                    }

                    // No support for pushdown of primary key check at this time.
                    // This isn't really a useful pushdown anyway.
                    if (hasContainers.stream().map(HasContainer::getKey).noneMatch(key -> key.equals(T.id.getAccessor()))) {
                        labels = hasStep.getLabels();
                        traversal.removeStep(hasStep);

                        // Cannot use sample strategy after HasStep at this time so break.
                        break;
                    } else {
                        hasContainers = new ArrayList<>();
                        break;
                    }
                } else {
                    // Unknown step, break.
                    break;
                }
            }
            traversal.addStep(index, new FireflyCompositeIdStepLocal(
                    traversal,
                    vertexStep.getDirection(),
                    vertexStep.getEdgeLabels(),
                    labels,
                    hasContainers,
                    graph.getBaseGraph().MOVEMENT_BARRIER_SIZE,
                    propertyKeys));

        }
    }

    public static FireflyCompositeEdgeIdLocalStrategy instance() {
        return new FireflyCompositeEdgeIdLocalStrategy();
    }
}
