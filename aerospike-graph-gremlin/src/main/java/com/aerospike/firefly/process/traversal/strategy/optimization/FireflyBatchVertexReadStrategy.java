package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.computer.util.ComputerHelper;
import com.aerospike.firefly.process.traversal.step.FireflyBatchVertexReadSampleLimitStep;
import com.aerospike.firefly.process.traversal.step.FireflyBatchVertexReadStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static com.aerospike.firefly.process.traversal.strategy.util.StrategyHelper.areEdgesRequired;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyBatchVertexReadStrategy extends FireflyStrategyBase {

    private static final Class[] INVALIDATING_STEP_CLASSES_ARRAY = INVALIDATING_STEP_CLASSES.toArray(new Class[]{});

    /**
     * Default constructor for FireflyCompositeEdgeIdStrategy.
     */
    public FireflyBatchVertexReadStrategy() {
    }

    @Override
    public String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_COMPOSITE_ID_STRATEGY;
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();

        if (ComputerHelper.onGraphComputer(traversal))
            return;
        final List<Step> steps = traversal.getSteps();

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

            final boolean propertyRemovalValid = !TraversalHelper.hasStepOfClass(traversal, INVALIDATING_STEP_CLASSES_ARRAY);

            // Replace vertex step with composite id step.
            traversal.removeStep(vertexStep);

            // Peek after the VertexStep to see if there is any HasStep's. If there are grab them all and push
            // them into the FireflyVertexStep.
            // Null has container is okay, it simply means that there is no HasContainer.
            // Note we don't want to push down ids.
            List<HasContainer> hasContainers = null;
            Set<String> labels = vertexStep.getLabels();
            int sampleSize = -1;
            long limitSize = -1;
            List<String> propertyKeys = null;

            // if any following steps (or there child) need vertex or edge then no optimization
            final boolean areEdgesRequired = areEdgesRequired(traversal, steps, index);

            while (labels.isEmpty()) {
                if (index >= steps.size()) {
                    break;
                }

                if (steps.get(index) instanceof NoOpBarrierStep) {
                    // Grab any labels and remove the barrier.
                    final NoOpBarrierStep<?> noOpBarrierStep = (NoOpBarrierStep<?>) steps.get(index);
                    labels = noOpBarrierStep.getLabels();
                    traversal.removeStep(steps.get(index));
                } else if (steps.get(index) instanceof PropertiesStep) {
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
                    if (hasContainers != null)
                        break;
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

                    labels = hasStep.getLabels();
                    traversal.removeStep(hasStep);
                } else if (steps.get(index) instanceof SampleGlobalStep) {
                    if (!graph.getBaseGraph().ENABLE_COMPOSITE_ID_SAMPLING_STRATEGY) {
                        break;
                    }
                    try {
                        // The sample size is private, need to use reflection to get it so the compiler doesn't complain.
                        final Field sampleField = SampleGlobalStep.class.getDeclaredField("amountToSample");
                        sampleField.setAccessible(true);

                        // Get the sample size.
                        sampleSize = (int) sampleField.get(steps.get(index));

                        // Check for labels that need to be propagated.
                        labels = ((SampleGlobalStep<?>) steps.get(index)).getLabels();

                        // Add limit step, otherwise each instance of the composite id step will sample the
                        // sample amount.
                        //
                        // This means if you sampled 30 and have 1000 items in and a barrier size of 100
                        // you would output 300 items, not 30, the limit breaks the barrier before they all run.
                        //
                        // This does bring the randomness of our step into question, however since our input
                        // is inherently unordered and out of our control, it seems sufficiently random
                        // for our purposes. Also, this can be disabled if truer randomness is needed.
                        traversal.removeStep(steps.get(index));
                        final RangeGlobalStep<?> step = new RangeGlobalStep<>(traversal.asAdmin(), 0, sampleSize);

                        // Labels should be propagated after the limit step.
                        if (!labels.isEmpty()) {
                            for (final String label : labels) {
                                step.addLabel(label);
                            }
                            labels.clear();
                        }
                        traversal.addStep(index, step);

                        // If sample comes before HasStep we are okay and don't need to break.
                    } catch (NoSuchFieldException | IllegalAccessException ignored) {
                        // Failed to get sample size, just ignore it.
                    }
                } else if (steps.get(index) instanceof RangeGlobalStep) {
                    if (sampleSize != -1 || limitSize != -1) {
                        // Already grabbed this, still looping to see if a HasStep is present, but we found this instead.
                        break;
                    }
                    if (!graph.getBaseGraph().ENABLE_BATCH_EDGE_READ_LIMIT_STRATEGY) {
                        break;
                    }
                    final long low = ((RangeGlobalStep<?>) steps.get(index)).getLowRange();
                    final long high = ((RangeGlobalStep<?>) steps.get(index)).getHighRange();

                    if (low != 0) {
                        break;
                    }

                    // Get the limit size.
                    limitSize = high;

                    // No need to add limit set since it's already there.
                } else {
                    // Unknown step, break.
                    break;
                }
            }
            if (sampleSize != -1 || limitSize != -1) {
                traversal.addStep(index, new FireflyBatchVertexReadSampleLimitStep(
                        traversal,
                        vertexStep.getDirection(),
                        vertexStep.getEdgeLabels(),
                        labels,
                        hasContainers,
                        sampleSize,
                        limitSize,
                        graph.getBaseGraph().MOVEMENT_BARRIER_SIZE,
                        propertyKeys,
                        areEdgesRequired));
            } else {
                traversal.addStep(index, new FireflyBatchVertexReadStep(
                        traversal,
                        vertexStep.getDirection(),
                        vertexStep.getEdgeLabels(),
                        labels,
                        hasContainers,
                        graph.getBaseGraph().MOVEMENT_BARRIER_SIZE,
                        propertyKeys,
                        areEdgesRequired));
            }
        }
    }
}
