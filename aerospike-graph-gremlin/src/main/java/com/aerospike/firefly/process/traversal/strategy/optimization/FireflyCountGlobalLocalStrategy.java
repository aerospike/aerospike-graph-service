package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.step.FireflyCacheGCStep;
import com.aerospike.firefly.process.traversal.step.map.FireflyCountGlobalLocalStep;
import com.aerospike.firefly.process.traversal.step.map.FireflyCountGlobalStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.CountGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.AggregateGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IdentityStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class FireflyCountGlobalLocalStrategy  extends FireflyStrategyBase {
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
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!traversal.isRoot()) {
            final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();
            if (!graph.getBaseGraph().ENABLE_EMBEDDED_GRAPH_COUNT_STRATEGY) {
                return;
            }
        }

        //if (TraversalHelper.onGraphComputer(traversal))
        //    return;
        for (int i = 0; i < traversal.getSteps().size(); i++) {
            if (traversal.getSteps().get(i) instanceof CountGlobalStep) {
                if (i > 0 && traversal.getSteps().get(i - 1) instanceof VertexStep) {
                    final VertexStep vertexStep = (VertexStep) traversal.getSteps().get(i - 1);
                    if (vertexStep.getLabels().isEmpty()) {
                        // Need to replace count step and remove vertex step
                        TraversalHelper.replaceStep(
                                traversal.getSteps().get(i),
                                new FireflyCountGlobalLocalStep<>(
                                        traversal, vertexStep.getDirection(), traversal.getSteps().get(i).getLabels()),
                                traversal);
                        traversal.removeStep(vertexStep);
                    }

                }
            }
    }
    }
}
