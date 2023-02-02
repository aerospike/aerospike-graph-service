package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.io.PrefetchTask;
import com.aerospike.firefly.io.impl.SubgraphPrefetchTask;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Grant Haywood <a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyPrefetchStrategy extends FireflyStrategyBase {
    private static final Set<Class<? extends PrefetchTask>> prefetchTasks = new HashSet<>() {{
        add(SubgraphPrefetchTask.class);
    }};

    /**
     * Default constructor for FireflyPrefetchStrategy.
     */
    public FireflyPrefetchStrategy() {
    }

    @Override
    protected String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_PREFETCH_STRATEGY;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // Disable prefetch strategy for time being.
    }
}
