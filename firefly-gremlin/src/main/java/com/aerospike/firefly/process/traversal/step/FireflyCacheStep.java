package com.aerospike.firefly.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;

import java.util.NoSuchElementException;
import java.util.UUID;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyCacheStep extends AbstractStep {
    //This step stores the cacheId
    public final UUID cacheId;

    public FireflyCacheStep(Traversal.Admin traversal, UUID cacheId) {
        super(traversal);
        this.cacheId = cacheId;
    }

    @Override
    protected Traverser.Admin processNextStart() throws NoSuchElementException {
        return null;
    }
    @Override
    public String toString(){
        return this.getClass().getSimpleName() + ":" + cacheId.toString();
    }
}
