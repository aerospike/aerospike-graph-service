package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
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
    private final FireflyStrategyBase fireflyPrefetchStrategy;
    private final FireflyStrategyBase fireflyReadThroughCacheStrategy;
    private final FireflyStrategyBase fireflyVertexEdgeLocalCountStrategy;

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
        this.fireflyPrefetchStrategy = new FireflyPrefetchStrategy();
        this.fireflyReadThroughCacheStrategy = new FireflyReadThroughCacheStrategy();
        this.fireflyVertexEdgeLocalCountStrategy = new FireflyVertexEdgeLocalCountStrategy();
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
            if (strategy.isEnabled((FireflyGraph) traversal.getGraph().get())) {
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

        // Drop step first.
        applyStrategy(traversal, fireflyGraphDropStrategy);

        // Steps that are generally applicable to most all traversals.
        applyStrategy(traversal, fireflyGraphStepStrategy);
        applyStrategy(traversal, fireflyReadThroughCacheStrategy);
        applyStrategy(traversal, fireflyPrefetchStrategy);

        // Steps that replace specific internal steps.
        applyStrategy(traversal, fireflyMergeStepStrategy);
        applyStrategy(traversal, fireflyCompositeEdgeIdStrategy);
        applyStrategy(traversal, fireflyBatchEdgeReadStrategy);
        applyStrategy(traversal, fireflyVertexEdgeLocalCountStrategy);
        applyStrategy(traversal, fireflyGraphCountStrategy);
    }
}
