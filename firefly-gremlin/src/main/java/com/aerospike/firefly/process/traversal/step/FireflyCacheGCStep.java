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

import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyCacheGCStep extends AbstractStep {
    public final UUID cacheId;
    private final Logger LOG = LoggerFactory.getLogger(FireflyCacheGCStep.class);

    public FireflyCacheGCStep(Traversal.Admin traversal, UUID cacheId) {
        super(traversal);
        this.cacheId = cacheId;
    }

    @Override
    protected Traverser.Admin processNextStart() throws NoSuchElementException {
        try {
            Traverser.Admin next = this.starts.next();
            if (!this.starts.hasNext()) {
                //This is the end of the traversal, remove the traversal cache
                TraversalCache it = ((FireflyGraph) traversal.getGraph().get())
                        .getBaseGraph()
                        .traversalCacheSet
                        .remove(cacheId);
                LOG.info("GC traversal cache {} with {} hits {} misses and {} entries",
                        cacheId,
                        it.getHitCount(),
                        it.getMissCount(),
                        it.size());
            }
            return next;
        } catch ( NoSuchElementException e) {
            ((FireflyGraph) traversal.getGraph().get()).getBaseGraph().traversalCacheSet.remove(cacheId);
            throw e;
        }

    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + ":" + cacheId.toString();
    }
}
