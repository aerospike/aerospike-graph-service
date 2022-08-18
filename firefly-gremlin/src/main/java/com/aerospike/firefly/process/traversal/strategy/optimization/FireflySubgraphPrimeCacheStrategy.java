package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.client.Key;
import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.process.traversal.step.FireflyCacheStep;
import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
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
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author Grant Haywood <a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflySubgraphPrimeCacheStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    Logger LOG = LoggerFactory.getLogger(FireflySubgraphPrimeCacheStrategy.class);
    private static final FireflySubgraphPrimeCacheStrategy INSTANCE = new FireflySubgraphPrimeCacheStrategy();

    private FireflySubgraphPrimeCacheStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if(!traversal.getGraph().isPresent())
            return;
        if(!FireflyGraph.class.isAssignableFrom(traversal.getGraph().get().getClass()))
            return;
        final AerospikeConnection db = ((FireflyGraph) traversal.getGraph().get()).getBaseGraph();
        if (TraversalHelper.onGraphComputer(traversal))
            return;
        if(!GraphStep.class.isAssignableFrom(traversal.getStartStep().getClass()))
            return;

        //Do we have a specific starting point? if not, don't run
        if (((GraphStep) traversal.getStartStep()).getIds() == null || ((GraphStep) traversal.getStartStep()).getIds().length != 1)
            return;

        List<Step> outSteps = traversal.getSteps().stream().filter(step -> VertexStep.class.isAssignableFrom(step.getClass())).filter(vertexStep -> {
            return ((VertexStep) vertexStep).getDirection() == Direction.OUT;
        }).collect(Collectors.toList());

        //@todo this is to simple, it should be 2 out steps off the starting point
        if (outSteps.size() < 2)
            return;

        UUID cacheId = UUID.randomUUID();
        LOG.info("will init cache with id: " + cacheId);
        Object startVertexId = ((GraphStep) traversal.getStartStep()).getIds()[0];
        Key[] cacheKeys = db.primeSubgraphCache((FireflyGraph) traversal.getGraph().get(), startVertexId);
        final FireflyCacheStep cacheStep = new FireflyCacheStep(traversal, cacheId, cacheKeys);
        traversal.addStep(0, cacheStep);
    }

    public static FireflySubgraphPrimeCacheStrategy instance() {
        return INSTANCE;
    }
}
