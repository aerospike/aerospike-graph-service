package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.step.map.FireflyCountGlobalLocalStep;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.CountStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.Collections;
import java.util.List;

public class FireflyCountGlobalLocalStrategy extends FireflyStrategyBase {
    /**
     * Default constructor for FireflyGraphCountStrategy.
     */
    public FireflyCountGlobalLocalStrategy() {
    }

    @Override
    public String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_FAST_COUNT_STRATEGY;
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getSteps().size() < 2) {
            CountStrategy.instance().apply(traversal);
            return;
        }

        for (int i = 1; i < traversal.getSteps().size(); i++) {
            if (traversal.getSteps().get(i) instanceof CountGlobalStep) {
                boolean isSupernodeSteppingValid = true;
                int stepsToRemove = 0;
                RangeGlobalStep rangeGlobalStep = null;
                HasStep hasStep = null;

                // vertex.out().limit(1).count()
                if (i > 1 && traversal.getSteps().get(i - 1) instanceof RangeGlobalStep) {
                    isSupernodeSteppingValid = false;
                    rangeGlobalStep = (RangeGlobalStep) traversal.getSteps().get(i - 1);
                    // is it valid range step?
                    if (rangeGlobalStep.getLowRange() != 0) {
                        continue;
                    }
                    stepsToRemove++;
                }

                // vertex.out().hasId(1).count()
                if (i > 1 + stepsToRemove && traversal.getSteps().get(i - 1 - stepsToRemove) instanceof HasStep) {
                    isSupernodeSteppingValid = false;
                    hasStep = (HasStep) traversal.getSteps().get(i - 1 - stepsToRemove);
                    if (!isValid(hasStep)) {
                        continue;
                    }
                    stepsToRemove++;
                }

                if (i > stepsToRemove && traversal.getSteps().get(i - 1 - stepsToRemove) instanceof VertexStep) {
                    final VertexStep vertexStep = (VertexStep) traversal.getSteps().get(i - 1 - stepsToRemove);
                    if (isSupernodeSteppingValid) {
                        final int idx = i - stepsToRemove - 2;
                        if (idx >= 0 && traversal.getSteps().get(idx) instanceof GraphStep) {
                            // g.V(<single id>).in/out().count(), do not optimize, we will use supernode stepping instead.
                            final GraphStep graphStep = (GraphStep) traversal.getSteps().get(idx);
                            if (graphStep.returnsVertex() && vertexStep.returnsEdge() && graphStep.getIds().length == 1) {
                                continue;
                            }
                        }
                    }
                    if (vertexStep.getLabels().isEmpty()) {
                        final long limit = rangeGlobalStep == null ? -1 : rangeGlobalStep.getHighRange();
                        final List<HasContainer> hasContainers = hasStep == null ? Collections.emptyList() : hasStep.getHasContainers();

                        // Need to replace count step and remove vertex and count steps
                        TraversalHelper.replaceStep(
                                traversal.getSteps().get(i),
                                new FireflyCountGlobalLocalStep<>(
                                        traversal, vertexStep.getDirection(), traversal.getSteps().get(i).getLabels(),
                                        vertexStep.getEdgeLabels(), limit, hasContainers),
                                traversal);

                        if (rangeGlobalStep != null) {
                            traversal.removeStep(rangeGlobalStep);
                        }
                        if (hasStep != null) {
                            traversal.removeStep(hasStep);
                        }
                        traversal.removeStep(vertexStep);

                        resetChild(traversal);
                    }
                }
            }
        }

        // todo: GRAPH-1501
        CountStrategy.instance().apply(traversal);
    }

    private boolean isValid(final HasStep hasStep) {
        for (HasContainer hasContainer : (List<HasContainer>) hasStep.getHasContainers()) {
            // only support filter by id for now
            if (!hasContainer.getKey().equals(T.id.getAccessor())) {
                return false;
            }
        }
        return true;
    }

    // workaround for some barrier steps
    private void resetChild(final Traversal.Admin<?, ?> traversal) {
        if (!traversal.isRoot()) {
            try {
                traversal.getParent().replaceLocalChild(traversal, traversal);
            } catch (final IllegalStateException ignored) {
                // some steps don't support child replacement
            }
        }
    }
}
