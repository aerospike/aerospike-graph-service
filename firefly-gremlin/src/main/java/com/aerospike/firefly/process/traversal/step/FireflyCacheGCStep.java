package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.io.impl.TraversalCache;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyCacheGCStep extends AbstractStep {
    private static final List<BiFunction<UUID, TraversalCache, Void>> gcHooks = new ArrayList<>();
    public final UUID cacheId;
    private final Logger LOG = LoggerFactory.getLogger(FireflyCacheGCStep.class);

    public FireflyCacheGCStep(Traversal.Admin traversal, UUID cacheId) {
        super(traversal);
        this.cacheId = cacheId;
    }

    public static void registerGCHook(BiFunction<UUID, TraversalCache, Void> gcCallback) {
        gcHooks.add(gcCallback);
    }

    public static void clearGCHooks() {
        gcHooks.clear();
    }

    @Override
    protected Traverser.Admin processNextStart() throws NoSuchElementException {
        try {
            Traverser.Admin next = this.starts.next();
            if (!this.starts.hasNext()) {
                //This is the end of the traversal, remove the traversal cache
                TraversalCache traversalCache = ((FireflyGraph) traversal.getGraph().get())
                        .getBaseGraph()
                        .traversalCacheSet
                        .remove(cacheId);

                ((FireflyGraph) traversal.getGraph().get())
                        .getBaseGraph()
                        .cacheTasks
                        .stream()
                        .filter(it -> it.getKey() == cacheId)
                        .forEach(entry -> {
                            if (!entry.getValue().isDone())
                                entry.getValue().cancel(true);
                        });


                LOG.debug("GC traversal cache {} with {} hits {} misses and {} entries",
                        cacheId,
                        traversalCache.getHitCount(),
                        traversalCache.getMissCount(),
                        traversalCache.size());
                gcHooks.forEach(cb -> {
                    cb.apply(cacheId, traversalCache);
                });

            }
            return next;
        } catch (NoSuchElementException e) {
            ((FireflyGraph) traversal.getGraph().get()).getBaseGraph().traversalCacheSet.remove(cacheId);
            throw e;
        }

    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + ":" + cacheId.toString();
    }
}
