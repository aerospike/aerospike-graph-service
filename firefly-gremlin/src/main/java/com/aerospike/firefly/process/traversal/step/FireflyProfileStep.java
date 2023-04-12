package com.aerospike.firefly.process.traversal.step;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Profiling;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;

import java.util.NoSuchElementException;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyProfileStep<S> extends AbstractStep<S, S> implements Profiling {
    public FireflyProfileStep(Traversal.Admin traversal) {
        super(traversal);
    }

    @Override
    public void setMetrics(final MutableMetrics parentMetrics) {
        if (parentMetrics != null) {
            parentMetrics.setAnnotation("bogusAnnotation", 100);
        }
    }

    @Override
    protected Traverser.Admin<S> processNextStart() throws NoSuchElementException {
        if (this.starts.hasNext()) {
            return this.starts.next();
        } else {
            throw FastNoSuchElementException.instance();
        }
    }
}
