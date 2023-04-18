package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;

import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Simon Zhao (<a href="https://www.linkedin.com/in/simonthezhao/</a>)
 */
public class FireflyDropStep extends AbstractStep {
    private final AtomicBoolean isDone;

    public FireflyDropStep(final Traversal.Admin traversal) {
        super(traversal);
        this.isDone = new AtomicBoolean(false);
    }

    @Override
    protected Traverser.Admin processNextStart() throws NoSuchElementException {
        if (!isDone.getAndSet(true)) {
            final FireflyGraph graph = (FireflyGraph) this.getTraversal().getGraph().get();
            graph.getBaseGraph().dropDatabase(graph, false);
        }
        throw FastNoSuchElementException.instance();
    }
}
