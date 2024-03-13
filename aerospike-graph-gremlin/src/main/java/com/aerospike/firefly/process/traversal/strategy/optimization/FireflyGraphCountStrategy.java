package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.step.FireflyCacheGCStep;
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

/**
 * This strategy will do a direct {@link org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerHelper#getVertices}
 * size call if the traversal is a count of the vertices and edges of the graph or a one-to-one map chain thereof.
 *
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Grant Haywood <a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 * @example <pre>
 * g.V().count()               // is replaced by TinkerCountGlobalStep
 * g.V().map(out()).count()    // is replaced by TinkerCountGlobalStep
 * g.E().label().count()       // is replaced by TinkerCountGlobalStep
 * </pre>
 */
public final class FireflyGraphCountStrategy extends FireflyStrategyBase {
    /**
     * Default constructor for FireflyGraphCountStrategy.
     */
    public FireflyGraphCountStrategy() {
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

        if (TraversalHelper.onGraphComputer(traversal))
            return;
        final List<Step> steps = new ArrayList<>(traversal.getSteps());
        steps.removeIf(step -> step.getClass().equals(FireflyCacheGCStep.class));

        // Must be at least GraphStep and CountGlobalStep.
        if (steps.size() < 2 || !(steps.get(0) instanceof GraphStep))
            return;

        final GraphStep<?, ?> graphStep = (GraphStep<?, ?>) steps.get(0);
        if (graphStep.getIds() == null || graphStep.getIds().length != 0)
            return;

        if (!(steps.get(steps.size() - 1) instanceof CountGlobalStep)) {
            return;
        }

        int hasStepCount = 0;
        HasStep<?> hasStep = null;
        for (int i = 1; i < steps.size() - 1; i++) {
            final Step<?, ?> step = steps.get(i);
            if ((steps.get(i) instanceof HasStep)) {
                hasStep = (HasStep<?>) steps.get(i);
                hasStepCount++;
            } else if (
                    !(step instanceof IdentityStep ||
                      step instanceof NoOpBarrierStep ||
                      step instanceof CollectingBarrierStep) ||
                     (step instanceof TraversalParent &&
                            TraversalHelper.anyStepRecursively(s -> (
                                    s instanceof SideEffectStep ||
                                    s instanceof AggregateGlobalStep),
                                    (TraversalParent) step)))
                return;
        }

        Class<? extends Element> returnClass = graphStep.getReturnClass();

        // If there is more than 1 HasStep or if there is any has step and
        // were returning an edge then we cannot use this strategy.
        if (hasStepCount > 1 ||
                (hasStep != null && Edge.class.isAssignableFrom(returnClass))) {
            return;
        }

        final List<HasContainer> aerospikeHasContainers =
                (hasStep == null) ? List.of() : FireflyBatchReadHelper.getAerospikeHasContainers(
                        FireflyBatchReadHelper.getHasContainersWithCardinalityOrder((FireflyGraph)
                                traversal.getGraph().get(), returnClass, hasStep.getHasContainers()));

        // Only support if all containers can be pushed to aerospike.
        if (hasStep != null && hasStep.getHasContainers().size() > 0 &&
                aerospikeHasContainers.size() != hasStep.getHasContainers().size()) {
            return;
        }

        TraversalHelper.removeAllSteps(traversal);
        traversal.addStep(new FireflyCountGlobalStep<>(traversal, returnClass, aerospikeHasContainers));
    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPost() {
        return Collections.singleton(FireflyGraphStepStrategy.class);
    }
}
