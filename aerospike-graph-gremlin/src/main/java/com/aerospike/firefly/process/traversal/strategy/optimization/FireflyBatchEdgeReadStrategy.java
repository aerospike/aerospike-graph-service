package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.step.FireflyBatchEdgeReadStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyBatchEdgeReadStrategy extends FireflyStrategyBase {

    final ThreadLocal<Boolean> rootGroup = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    /**
     * Default constructor for FireflyBatchEdgeReadStrategy.
     */
    public FireflyBatchEdgeReadStrategy() {
    }

    @Override
    public String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_BATCH_EDGE_READ_STRATEGY;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();

        // Reset whenever root.
        if (traversal.isRoot()) {
            rootGroup.set(false);
        }

        if (!traversal.isRoot()) {
            if (rootGroup.get()) {
                return;
            }
            if (!graph.getBaseGraph().ENABLE_EMBEDDED_BATCH_EDGE_READ_STRATEGY) {
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
            if (!vertexStep.returnsEdge()) {
                continue;
            }
            traversal.removeStep(vertexStep);

            // Peek after the VertexStep to see if there is any HasStep's. If there are grab them all and push
            // them into the FireflyVertexStep.
            // Null has container is okay, it simply means that there is no HasContainer.
            // Note we don't want to push down ids.
            List<HasContainer> hasContainers = null;
            Set<String> labels = vertexStep.getLabels();


            int sampleSize = -1;

            while (labels.isEmpty()) {
                if (index >= steps.size()) {
                    break;
                }
                if (steps.get(index) instanceof NoOpBarrierStep) {
                    // Grab any labels and remove the barrier.
                    final NoOpBarrierStep<?> noOpBarrierStep = (NoOpBarrierStep<?>) steps.get(index);
                    labels = noOpBarrierStep.getLabels();
                    traversal.removeStep(steps.get(index));
                } else if (steps.get(index) instanceof HasStep) {
                    // Grab has containers and push them down.
                    final HasStep<?> hasStep = (HasStep<?>) steps.get(index);
                    hasContainers = hasStep.getHasContainers();

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
                } else if (steps.get(index) instanceof SampleGlobalStep) {
                    if (!graph.getBaseGraph().ENABLE_BATCH_EDGE_READ_SAMPLING_STRATEGY) {
                        break;
                    }
                    try {
                        // The sample size is private, need to use reflection to get it so the compiler doesn't complain.
                        final Field sampleField = SampleGlobalStep.class.getDeclaredField("amountToSample");
                        sampleField.setAccessible(true);

                        // Get the sample size.
                        sampleSize = (int) sampleField.get(steps.get(index));

                        // Check for labels that need to be propagated.
                        if (!((SampleGlobalStep<?>) steps.get(index)).getLabels().isEmpty()) {
                            labels = ((SampleGlobalStep<?>) steps.get(index)).getLabels();
                        }

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
                            for (final String label: labels) {
                                step.addLabel(label);
                            }
                            labels.clear();
                        }
                        traversal.addStep(index, step);

                        // If sample comes before HasStep we are okay and don't need to break.
                    } catch (NoSuchFieldException | IllegalAccessException ignored) {
                        // Failed to get sample size, just ignore it.
                    }
                } else {
                    // Unknown step, break.
                    break;
                }
            }

            // Replace vertex step with composite id step.
            traversal.addStep(index, new FireflyBatchEdgeReadStep(
                    traversal,
                    vertexStep.getDirection(),
                    vertexStep.getEdgeLabels(),
                    labels,
                    hasContainers,
                    sampleSize,
                    graph.getBaseGraph().MOVEMENT_BARRIER_SIZE));
        }
    }
}
