package com.aerospike.firefly.process.traversal.step.map;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.util.FireflyHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;

/**
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 */
public class FireflyCountGlobalStep<S extends Element> extends AbstractStep<S, Long> {

    private final Class<S> elementClass;
    private boolean done = false;

    public FireflyCountGlobalStep(final Traversal.Admin traversal, final Class<S> elementClass) {
        super(traversal);
        this.elementClass = elementClass;
    }

    @Override
    protected Traverser.Admin<Long> processNextStart() throws NoSuchElementException {
        if (!this.done) {
            this.done = true;
            final FireflyGraph graph = (FireflyGraph) this.getTraversal().getGraph().get();
            return this.getTraversal().getTraverserGenerator().generate(Vertex.class.isAssignableFrom(this.elementClass) ?
                            FireflyHelper.countVertices(graph) : FireflyHelper.countEdges(graph),
                    (Step) this, 1L);
        } else
            throw FastNoSuchElementException.instance();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, this.elementClass.getSimpleName().toLowerCase());
    }

    @Override
    public int hashCode() {
        return super.hashCode() ^ this.elementClass.hashCode();
    }

    @Override
    public void reset() {
        this.done = false;
    }
}
