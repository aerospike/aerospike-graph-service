package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;

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
     * strategy is always enabled.
     *
     * @return Boolean true if enabled, false otherwise.
     */
    public boolean isEnabled(final FireflyGraph fireflyGraph) {
        // If strategy has no enabled key, then it is always enabled.
        final String enabledKey = getStrategyEnabledKey();
        if (enabledKey == null) {
            return true;
        }

        // Check if the strategy is enabled.
        return Boolean.parseBoolean(ConfigurationHelper.getOrDefault(enabledKey, fireflyGraph.configuration()));
    }
}
