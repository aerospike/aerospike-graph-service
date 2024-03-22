package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.io.FireflyCache;
import com.aerospike.firefly.security.UserContext;
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
public class FireflyUserContextStep extends AbstractStep {
    private final Logger LOG = LoggerFactory.getLogger(FireflyUserContextStep.class);
    private UserContext userContext;

    public FireflyUserContextStep(final Traversal.Admin traversal, final UserContext userContext, final Set<String> labels) {
        super(traversal);
        this.userContext = userContext;
        this.labels = labels;
    }

    @Override
    protected Traverser.Admin processNextStart() throws NoSuchElementException {
        if (userContext != null || !userContext.valid((FireflyGraph) traversal.getGraph().get())) {
            throw new RuntimeException("Invalid user context");
        }
        if (this.starts.hasNext()) {
            return this.starts.next();
        } else {
            throw FastNoSuchElementException.instance();
        }
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + ":" + userContext;
    }
}
