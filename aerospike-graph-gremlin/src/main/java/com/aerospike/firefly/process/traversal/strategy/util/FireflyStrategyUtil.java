package com.aerospike.firefly.process.traversal.strategy.util;

import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyAdjacentVertexIdStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyAuthenticationStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyBatchEdgeReadLocalStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyBatchEdgeReadStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyBatchTraversalFilterStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyBatchVertexReadLocalStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyBatchVertexReadStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyComputerStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyCountGlobalLocalStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyEdgeToVertexBatchReadStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyElementMapStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyGraphCountStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyGraphDropStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyGraphStepStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyHasIdVertexFilterStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyMergeStepStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyOtherVBatchReadStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyReadThroughCacheStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflySchemaResetStrategy;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyStrategyBase;
import com.aerospike.firefly.process.traversal.strategy.optimization.FireflyVertexEdgeLocalCountStrategy;
import com.aerospike.firefly.process.traversal.strategy.options.FireflyTraversalOptionsStrategy;
import com.aerospike.firefly.process.traversal.strategy.profile.FireflyQueryTracingStrategy;
import com.aerospike.firefly.process.traversal.strategy.profile.FireflyScanProfileStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class FireflyStrategyUtil {
    static public List<FireflyStrategyBase> FIREFLY_STRATEGIES = new ArrayList<>();
    static public Map<Class<? extends FireflyStrategyBase>, StrategyOrdering> STRATEGY_ORDER = new HashMap<>();

    // Register any custom Firefly Strategies here in the order that they should be executed.
    static {
        // Perform auth strategy before we mutate anything.
        FIREFLY_STRATEGIES.add(new FireflyAuthenticationStrategy());

        // Apply relevant TraversalOptions to Firefly.
        FIREFLY_STRATEGIES.add(new FireflyTraversalOptionsStrategy());

        // Steps that override the entire step list first.
        FIREFLY_STRATEGIES.add(new FireflyGraphDropStrategy());
        FIREFLY_STRATEGIES.add(FireflyComputerStrategy.instance());
        FIREFLY_STRATEGIES.add(new FireflyCountGlobalLocalStrategy());
        FIREFLY_STRATEGIES.add(new FireflyGraphCountStrategy());

        // This step replaces out/in.id() with single step.
        FIREFLY_STRATEGIES.add(new FireflyAdjacentVertexIdStrategy());

        // Steps that are generally applicable to most all traversals.
        FIREFLY_STRATEGIES.add(new FireflyGraphStepStrategy());
        FIREFLY_STRATEGIES.add(new FireflyReadThroughCacheStrategy());
        FIREFLY_STRATEGIES.add(new FireflySchemaResetStrategy());

        // This step places an out.count() or in.count() step, therefore must happen between composite id and batch read.
        FIREFLY_STRATEGIES.add(new FireflyVertexEdgeLocalCountStrategy());

        // Steps that replace specific internal steps.
        FIREFLY_STRATEGIES.add(new FireflyHasIdVertexFilterStrategy());
        FIREFLY_STRATEGIES.add(new FireflyMergeStepStrategy());
        FIREFLY_STRATEGIES.add(new FireflyBatchVertexReadStrategy());
        FIREFLY_STRATEGIES.add(new FireflyBatchVertexReadLocalStrategy());
        FIREFLY_STRATEGIES.add(new FireflyBatchEdgeReadStrategy());
        FIREFLY_STRATEGIES.add(new FireflyOtherVBatchReadStrategy());
        FIREFLY_STRATEGIES.add(new FireflyBatchEdgeReadLocalStrategy());
        FIREFLY_STRATEGIES.add(new FireflyEdgeToVertexBatchReadStrategy());
        // Must run after all batch read strategies so that LocalBarrier steps
        // (FireflyBatchEdgeReadStep, FireflyBatchVertexReadStep, etc.) are already
        // present inside TraversalFilterStep child traversals when this strategy inspects them.
        FIREFLY_STRATEGIES.add(new FireflyBatchTraversalFilterStrategy());
        FIREFLY_STRATEGIES.add(new FireflyElementMapStrategy());

        // Steps that collect metrics.
        FIREFLY_STRATEGIES.add(new FireflyScanProfileStrategy());
        FIREFLY_STRATEGIES.add(new FireflyQueryTracingStrategy());

        for (int i = 0; i < FIREFLY_STRATEGIES.size(); i++) {
            final Class<? extends FireflyStrategyBase> prior;
            final Class<? extends FireflyStrategyBase> post;
            if (i == 0) {
                prior = null;
            } else {
                prior = FIREFLY_STRATEGIES.get(i - 1).getClass();
            }
            if (i == FIREFLY_STRATEGIES.size() - 1) {
                post = null;
            } else {
                post = FIREFLY_STRATEGIES.get(i + 1).getClass();
            }
            STRATEGY_ORDER.put(FIREFLY_STRATEGIES.get(i).getClass(), new StrategyOrdering(prior, post));

        }
    }

    public static void resetStrategies() {
        FIREFLY_STRATEGIES.forEach(FireflyStrategyBase::resetIsEnabled);
    }

    static public class StrategyOrdering {
        public final Set<Class<? extends TraversalStrategy.ProviderOptimizationStrategy>> prior;
        public final Set<Class<? extends TraversalStrategy.ProviderOptimizationStrategy>> post;

        private StrategyOrdering(final Class<? extends FireflyStrategyBase> prior,
                                 final Class<? extends FireflyStrategyBase> post) {
            this.prior = prior == null ? Set.of() : Set.of(prior);
            this.post = post == null ? Set.of() : Set.of(post);
        }
    }
}
