package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.process.traversal.step.FireflyCacheGCStep;
import com.aerospike.firefly.process.traversal.step.FireflyCacheStep;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author Grant Haywood <a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyAsyncPrefetchStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    Logger LOG = LoggerFactory.getLogger(FireflyAsyncPrefetchStrategy.class);
    private static final FireflyAsyncPrefetchStrategy INSTANCE = new FireflyAsyncPrefetchStrategy();

    private FireflyAsyncPrefetchStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {



        if (!FireflyAsyncPrefetchStrategy.Util.isCacheableTraversal(traversal))
            return;
        final AerospikeConnection db = ((FireflyGraph) traversal.getGraph().get()).getBaseGraph();
        List<Step> outSteps =
                traversal
                        .getSteps()
                        .stream()
                        .filter(step ->
                                VertexStep.class.isAssignableFrom(step.getClass()))
                        .filter(vertexStep ->
                                ((VertexStep<?>) vertexStep).getDirection() == Direction.OUT)
                        .collect(Collectors.toList());
        //@todo this is to simple, it should be 2 out steps off the starting point
        if (outSteps.size() < 2)
            return;
        UUID cacheId = UUID.randomUUID();
        LOG.info("will init cache with id: " + cacheId);
        Object startVertexId = ((GraphStep) traversal.getStartStep()).getIds()[0];
        final FireflyCacheStep cacheStep = new FireflyCacheStep(traversal, cacheId, null);
        final FireflyCacheGCStep gcStep = new FireflyCacheGCStep(traversal, cacheId);
        traversal.addStep(0, cacheStep);
        traversal.addStep(traversal.getSteps().size(), gcStep);
        db.currentTraversal.set(traversal);
        db.primeSubgraphCache(cacheId, (FireflyGraph) traversal.getGraph().get(), startVertexId);
    }

    public static FireflyAsyncPrefetchStrategy instance() {
        return INSTANCE;
    }

    public static class Util {
        protected static boolean isCacheableTraversal(Traversal.Admin traversal) {
            if (!traversal.getGraph().isPresent())
                return false;
            if (!FireflyGraph.class.isAssignableFrom(traversal.getGraph().get().getClass()))
                return false;
            if (TraversalHelper.onGraphComputer(traversal))
                return false;
            if (!GraphStep.class.isAssignableFrom(traversal.getStartStep().getClass()))
                return false;
            //Do we have a specific starting point? if not, don't run
            if (((GraphStep) traversal.getStartStep()).getIds() == null)
                return false;
            if (((GraphStep) traversal.getStartStep()).getIds().length != 1)
                return false;
            return true;
        }

        public static boolean isCachedTraversal(Traversal.Admin traversal) {
            if (FireflyCacheStep.class.isAssignableFrom(traversal.getStartStep().getClass()))
                return true;
            return false;
        }

        public static Optional<UUID> idFromTraversal(Traversal.Admin traversal) {
            if (!isCachedTraversal(traversal))
                return Optional.empty();
            return Optional.of(((FireflyCacheStep) traversal.getSteps().get(0)).cacheId);
        }
    }
}
