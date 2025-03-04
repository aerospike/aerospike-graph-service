package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.io.FireflyCache;
import com.aerospike.firefly.io.aerospike.AerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
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

    public FireflyCacheGCStep(final Traversal.Admin traversal, final Set<String> labels) {
        super(traversal);
        this.labels = labels;
    }

    @Override
    protected Traverser.Admin processNextStart() throws NoSuchElementException {

        Traverser.Admin t;
        if (this.starts.hasNext()) {
            t = starts.next();
        } else {
            throw FastNoSuchElementException.instance();
        }

        final AerospikeConnection db = ((FireflyGraph) traversal.getGraph().get()).getBaseGraph();
        final FireflyCache cache = db.transactionCache.get();
        final FireflyCache noPropsCache = db.emptyPropsTransactionCache.get();
        if (cache != null) {
            LOG.debug("Removing cache " + cache);
            LOG.trace("Cache hits: " + cache.getHitCount());
            LOG.trace("Cache misses: " + cache.getMissCount());
            cache.invalidateAll();
        }
        if (noPropsCache != null) {
            LOG.debug("Removing cache " + noPropsCache);
            LOG.trace("Cache hits: " + noPropsCache.getHitCount());
            LOG.trace("Cache misses: " + noPropsCache.getMissCount());
            noPropsCache.invalidateAll();
        }
        db.emptyPropsTransactionCache.remove();
        db.transactionCache.remove();

        return t;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName();
    }
}
