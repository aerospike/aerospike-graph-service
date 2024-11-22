package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.strategy.profile.FireflyScanProfileStrategy;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.AdjacentToIncidentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.optimization.IncidentToAdjacentStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.LambdaRestrictionStrategy;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyContentionHandlingStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    // All strategies.
    private final FireflyStrategyBase fireflyCompositeEdgeIdStrategy;
    private final FireflyStrategyBase fireflyBatchEdgeReadStrategy;
    private final FireflyStrategyBase fireflyGraphCountStrategy;
    private final FireflyStrategyBase fireflyGraphDropStrategy;
    private final FireflyStrategyBase fireflyGraphStepStrategy;
    private final FireflyStrategyBase fireflyMergeStepStrategy;
    private final FireflyStrategyBase fireflyReadThroughCacheStrategy;
    private final FireflyStrategyBase fireflyVertexEdgeLocalCountStrategy;
    private final FireflyStrategyBase fireflyScanProfileStrategy;
    private final FireflyStrategyBase fireflyAuthenticationStrategy;
    private final FireflyStrategyBase fireflyAdjacentVertexIdStrategy;
    private final FireflyStrategyBase fireflyBatchOtherVReadStrategy;
    private final FireflyStrategyBase fireflyEdgeToVertexBatchReadStrategy;
    private final FireflyStrategyBase fireflyCompositeEdgeIdLocalStrategy;
    private final FireflyStrategyBase fireflyBatchEdgeReadLocalStrategy;
    private final FireflyStrategyBase fireflyCountGlobalLocalStrategy;

    /**
     * Default constructor for FireflyContentionHandlingStrategy.
     */
    public FireflyContentionHandlingStrategy() {
        this.fireflyCompositeEdgeIdStrategy = new FireflyCompositeEdgeIdStrategy();
        this.fireflyBatchEdgeReadStrategy = new FireflyBatchEdgeReadStrategy();
        this.fireflyGraphCountStrategy = new FireflyGraphCountStrategy();
        this.fireflyGraphDropStrategy = new FireflyGraphDropStrategy();
        this.fireflyGraphStepStrategy = new FireflyGraphStepStrategy();
        this.fireflyMergeStepStrategy = new FireflyMergeStepStrategy();
        this.fireflyReadThroughCacheStrategy = new FireflyReadThroughCacheStrategy();
        this.fireflyVertexEdgeLocalCountStrategy = new FireflyVertexEdgeLocalCountStrategy();
        this.fireflyScanProfileStrategy = new FireflyScanProfileStrategy();
        this.fireflyAuthenticationStrategy = new FireflyAuthenticationStrategy();
        this.fireflyCompositeEdgeIdLocalStrategy = new FireflyCompositeEdgeIdLocalStrategy();
        this.fireflyBatchEdgeReadLocalStrategy = new FireflyBatchEdgeReadLocalStrategy();
        this.fireflyCountGlobalLocalStrategy = new FireflyCountGlobalLocalStrategy();
        this.fireflyAdjacentVertexIdStrategy = new FireflyAdjacentVertexIdStrategy();
        this.fireflyBatchOtherVReadStrategy = new FireflyOtherVBatchReadStrategy();
        this.fireflyEdgeToVertexBatchReadStrategy = new FireflyEdgeToVertexBatchReadStrategy();
    }

    /**
     * This function applies a TinkerPop strategy indiscriminately.
     *
     * @param traversal Traversal to apply strategy to.
     * @param strategy  Strategy to apply.
     */
    private void applyTinkerPopStrategy(final Traversal.Admin<?, ?> traversal, final AbstractTraversalStrategy<?> strategy) {
        if (traversal.getGraph().isPresent()) {
            strategy.apply(traversal);
        }
    }

    /**
     * This function applies the strategy if the strategy is enabled.
     *
     * @param traversal Traversal to apply strategy to.
     * @param strategy  Strategy to apply, if enabled.
     */
    private void applyStrategy(final Traversal.Admin<?, ?> traversal, final FireflyStrategyBase strategy) {
        if (traversal.getGraph().isPresent()) {
            if (traversal.isRoot()) {
                strategy.reset();
            }
            if ((traversal.getGraph().get() instanceof FireflyGraph) && strategy.isEnabled((FireflyGraph) traversal.getGraph().get())) {
                strategy.apply(traversal);
            }
        }
    }

    /**
     * Master arbitration of strategies is done in this function. Any necessary logic to determine whether a strategy
     * should execute or not should be done here.
     *
     * @param traversal Traversal to apply strategies on.
     */
    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // Look for Lambda functions for security reasons.
        // TinkerPop conveniently has a strategy for this.
        applyTinkerPopStrategy(traversal, LambdaRestrictionStrategy.instance());

        // Perform auth strategy before we mutate anything.
        applyStrategy(traversal, fireflyAuthenticationStrategy);

        // Steps that override the entire step list first.
        applyStrategy(traversal, fireflyGraphDropStrategy);
        applyStrategy(traversal, fireflyGraphCountStrategy);
        applyStrategy(traversal, fireflyCountGlobalLocalStrategy);

        // This step replaces out/in.id() with single step.
        applyTinkerPopStrategy(traversal, AdjacentToIncidentStrategy.instance());
        applyTinkerPopStrategy(traversal, IncidentToAdjacentStrategy.instance());
        applyStrategy(traversal, fireflyAdjacentVertexIdStrategy);

        // Steps that are generally applicable to most all traversals.
        applyStrategy(traversal, fireflyGraphStepStrategy);
        applyStrategy(traversal, fireflyReadThroughCacheStrategy);

        // This step places an out.count() or in.count() step, therefore must happen between composite id and batch read.
        applyStrategy(traversal, fireflyVertexEdgeLocalCountStrategy);

        // Steps that replace specific internal steps.
        applyStrategy(traversal, fireflyMergeStepStrategy);
        applyStrategy(traversal, fireflyCompositeEdgeIdStrategy);
        applyStrategy(traversal, fireflyCompositeEdgeIdLocalStrategy);
        applyStrategy(traversal, fireflyBatchEdgeReadStrategy);
        applyStrategy(traversal, fireflyBatchOtherVReadStrategy);
        applyStrategy(traversal, fireflyBatchEdgeReadLocalStrategy);
        applyStrategy(traversal, fireflyEdgeToVertexBatchReadStrategy);
        applyStrategy(traversal, fireflyScanProfileStrategy);
    }
}
