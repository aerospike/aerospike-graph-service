package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.io.AerospikeConnection;
import com.aerospike.firefly.io.PrefetchTask;
import com.aerospike.firefly.io.impl.SubgraphPrefetchTask;
import com.aerospike.firefly.io.impl.TraversalCache;
import com.aerospike.firefly.process.traversal.step.FireflyCacheGCStep;
import com.aerospike.firefly.process.traversal.step.FireflyCacheStep;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * @author Grant Haywood <a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyTraversalCacheStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> implements TraversalStrategy.ProviderOptimizationStrategy {
    Logger LOG = LoggerFactory.getLogger(FireflyTraversalCacheStrategy.class);
    private static final FireflyTraversalCacheStrategy INSTANCE = new FireflyTraversalCacheStrategy();
    private static final HashSet<Class<? extends PrefetchTask>> prefetchTasks = new HashSet<>() {{
        add(SubgraphPrefetchTask.class);
    }};

    private FireflyTraversalCacheStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!FireflyTraversalCacheStrategy.Util.isCacheableTraversal(traversal))
            return;
        final AerospikeConnection db = ((FireflyGraph) traversal.getGraph().get()).getBaseGraph();
        final UUID cacheId = UUID.randomUUID();
        final FireflyCacheStep cacheStep = new FireflyCacheStep(traversal, cacheId);
        traversal.addStep(0, cacheStep);
        //Find all the prefetch tasks that support this traversal
        List<Runnable> prefetchTasksThatMatchTraversal = prefetchTasks.stream().map(taskClass -> {
            final PrefetchTask val;
            try {
                val = ((PrefetchTask) taskClass
                        .getMethod("create")
                        .invoke(null));
            } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
            return val.getTask(traversal);
        }).filter(it -> it.isPresent()).map(Optional::get).collect(Collectors.toList());
        //If there are none, remove the CacheStep, it should run without alteration
        if (prefetchTasksThatMatchTraversal.size() == 0) {
            traversal.removeStep(0);
            return;
        }

        //Now that we know this is a supported traversal
        // Set the traversal thread-local reference
        db.currentTraversal.set(traversal);
        final TraversalCache traversalCache = new TraversalCache(db);
        //Create a cache for this traversal by id
        db.traversalCacheSet.put(cacheId, traversalCache);
        //Tack on the step that will remove the cache when its finished
        final FireflyCacheGCStep gcStep = new FireflyCacheGCStep(traversal, cacheId);
        traversal.addStep(traversal.getSteps().size(), gcStep);
        //Execute all the supported prefetch tasks
        prefetchTasks.forEach(taskClass -> {
            try {
                ((SubgraphPrefetchTask) taskClass
                        .getMethod("create")
                        .invoke(null))
                        .getTask(traversal)
                        .ifPresent(db::runPrefetchTask);
            } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public static FireflyTraversalCacheStrategy instance() {
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
