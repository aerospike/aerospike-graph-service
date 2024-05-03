package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.io.FireflyCache;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.NoSuchElementException;
import java.util.Set;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyCacheGCStep extends AbstractStep {
    private final Logger LOG = LoggerFactory.getLogger(FireflyCacheGCStep.class);
    private FireflyCache cache;
    private FireflyCache noPropsCache;

    public FireflyCacheGCStep(final Traversal.Admin traversal, final FireflyCache cache, final FireflyCache noPropsCache, final Set<String> labels) {
        super(traversal);
        this.cache = cache;
        this.labels = labels;
        this.noPropsCache = noPropsCache;
    }

    @Override
    protected Traverser.Admin processNextStart() throws NoSuchElementException {
        if (cache != null) {
            LOG.debug("Removing cache " + cache);
            LOG.trace("Cache hits: " + cache.getHitCount());
            LOG.trace("Cache misses: " + cache.getMissCount());
            cache.invalidateAll();
            cache = null;
        }
        if (noPropsCache != null) {
            LOG.debug("Removing cache " + noPropsCache);
            LOG.trace("Cache hits: " + noPropsCache.getHitCount());
            LOG.trace("Cache misses: " + noPropsCache.getMissCount());
            noPropsCache.invalidateAll();
            noPropsCache = null;
        }
        if (this.starts.hasNext()) {
            return this.starts.next();
        } else {
            throw FastNoSuchElementException.instance();
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + ":" + cache;
    }
}
