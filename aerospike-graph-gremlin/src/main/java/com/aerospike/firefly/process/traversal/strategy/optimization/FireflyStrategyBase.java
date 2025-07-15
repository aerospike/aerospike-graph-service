package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.strategy.util.FireflyStrategyUtil;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;

import java.util.Set;

import static com.aerospike.firefly.process.traversal.strategy.util.FireflyStrategyUtil.STRATEGY_ORDER;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public abstract class FireflyStrategyBase extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {



    /**
     * Default constructor for FireflyStrategyBase.
     */
    public FireflyStrategyBase() {
    }

    /**
     * Function to get key for configuration option that determines whether this is enabled or disabled this strategy.
     *
     * @return null, which means this strategy is always enabled.
     */
    protected String getStrategyEnabledKey() {
        return null;
    }

    /**
     * Function to determine whether this strategy is enabled or disabled. If getStrategyEnabledKey() returns null, this
     * strategy is always enabled unless this is overridden.
     *
     * @return Boolean true if enabled, false otherwise.
     */
    protected boolean isEnabled(final FireflyGraph graph) {
        final String enabledKey = getStrategyEnabledKey();
        return enabledKey == null || ConfigurationHelper.getOrDefaultBool(enabledKey, graph.configuration());
    }

    /**
     * Preprocessing check to ensure that the traversal is valid for a Firefly Strategy.
     *
     * @return Boolean true if valid for FireflyGraph.
     */
    private boolean isValidForFirefly(final Traversal.Admin<?, ?> traversal) {
        if (traversal.getGraph().isPresent() && traversal.getGraph().get() instanceof FireflyGraph) {
            final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();
            final boolean isEnabled = isEnabled(graph);
            if (isEnabled && traversal.isRoot()) {
                this.reset();
            }
            return isEnabled;
        } else {
            return false;
        }
    }

    protected void reset() {
    }

    /**
     * Wrapper to ensure that only enabled Firefly Strategies are invoked.
     *
     * @param traversal
     */
    @Override
    public final void apply(final Traversal.Admin<?, ?> traversal) {
        if (isValidForFirefly(traversal)) {
            doApply(traversal);
        }
    }

    protected abstract void doApply(final Traversal.Admin<?, ?> traversal);

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPrior() {
        final FireflyStrategyUtil.StrategyOrdering strategyOrdering = STRATEGY_ORDER.get(this.getClass());
        if (strategyOrdering == null) {
            // This should never happen.
            throw new IllegalStateException(this.getClass().getName() + " was not found in the registered strategies list.");
        }
        return strategyOrdering.prior;
    }

    @Override
    public Set<Class<? extends ProviderOptimizationStrategy>> applyPost() {
        final FireflyStrategyUtil.StrategyOrdering strategyOrdering = STRATEGY_ORDER.get(this.getClass());
        if (strategyOrdering == null) {
            // This should never happen.
            throw new IllegalStateException(this.getClass().getName() + " was not found in the registered strategies list.");
        }
        return strategyOrdering.post;
    }
}
