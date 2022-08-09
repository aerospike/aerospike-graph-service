package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.step.FireflyCacheStep;
import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author Grant Haywood <a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflySubgraphPurgeCacheStrategy extends AbstractTraversalStrategy<TraversalStrategy.FinalizationStrategy> implements TraversalStrategy.FinalizationStrategy {
    Logger LOG = LoggerFactory.getLogger(FireflySubgraphPurgeCacheStrategy.class);

    private static final FireflySubgraphPurgeCacheStrategy INSTANCE = new FireflySubgraphPurgeCacheStrategy();

    private FireflySubgraphPurgeCacheStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (TraversalHelper.onGraphComputer(traversal))
            return;


        if(FireflyCacheStep.class.isAssignableFrom(traversal.getSteps().get(0).getClass()))
            LOG.info("purge cache " + ((FireflyCacheStep)traversal.getSteps().get(0)).cacheId);

    }

    public static FireflySubgraphPurgeCacheStrategy instance() {
        return INSTANCE;
    }
}
