package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.PathFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public abstract class FireflyStrategyBase extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {

    protected static final Set<Class> INVALIDATING_STEP_CLASSES = new HashSet<>(Arrays.asList(
            PathStep.class, PathFilterStep.class, TreeStep.class, TreeSideEffectStep.class, LambdaHolder.class));

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

        final boolean value = enabledKey == null ? true : ConfigurationHelper.getOrDefaultBool(enabledKey, fireflyGraph.configuration());
        // Check if the strategy is enabled.
        return value;
    }

    public void reset() {
    }

    protected boolean isPropertyRemovalValid(final Traversal.Admin<?, ?> traversal) {
        final Traversal.Admin<?, ?> root = TraversalHelper.getRootTraversal(traversal);
        return !TraversalHelper.hasStepOfAssignableClassRecursively(INVALIDATING_STEP_CLASSES, root);
    }
}
