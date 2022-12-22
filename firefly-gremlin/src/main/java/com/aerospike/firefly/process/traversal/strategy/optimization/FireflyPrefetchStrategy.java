package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.io.PrefetchTask;
import com.aerospike.firefly.io.impl.SubgraphPrefetchTask;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;

import java.util.HashSet;
import java.util.Set;

/**
 * @author Grant Haywood <a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyPrefetchStrategy
        extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy>
        implements TraversalStrategy.ProviderOptimizationStrategy {
    private static final FireflyPrefetchStrategy INSTANCE = new FireflyPrefetchStrategy();
    private static final Set<Class<? extends PrefetchTask>> prefetchTasks = new HashSet<>() {{
        add(SubgraphPrefetchTask.class);
    }};

    private FireflyPrefetchStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // Disable prefetch strategy for time being.
    }

    public static FireflyPrefetchStrategy instance() {
        return INSTANCE;
    }
}
