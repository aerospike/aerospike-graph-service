package com.aerospike.firefly.process.traversal.step.map;

import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.FireflyHelper;
import com.aerospike.firefly.util.TimeoutHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.List;
import java.util.NoSuchElementException;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Grant Haywood (<a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyCountGlobalStep<S extends Element> extends AbstractStep<S, Long> {

    private final Class<S> elementClass;
    private final List<HasContainer> aerospikeHasContainers;
    private final String edgeLabel;
    private boolean done = false;

    public FireflyCountGlobalStep(final Traversal.Admin traversal, final Class<S> elementClass,
                                  final List<HasContainer> aerospikeHasContainers) {
        this(traversal, elementClass, aerospikeHasContainers, null);
    }

    public FireflyCountGlobalStep(final Traversal.Admin traversal, final Class<S> elementClass,
                                  final List<HasContainer> aerospikeHasContainers, final String edgeLabel) {
        super(traversal);
        this.elementClass = elementClass;
        this.aerospikeHasContainers = aerospikeHasContainers;
        this.edgeLabel = edgeLabel;
    }

    @Override
    protected Traverser.Admin<Long> processNextStart() throws NoSuchElementException {
        if (!this.done) {
            this.done = true;
            final FireflyGraph graph = (FireflyGraph) this.getTraversal().getGraph().get();
            final long evaluationTimeout = TimeoutHelper.calculate(traversal);
            final long count;
            if (Vertex.class.isAssignableFrom(this.elementClass)) {
                count = FireflyHelper.countVertices(graph, aerospikeHasContainers, evaluationTimeout);
            } else if (edgeLabel != null) {
                count = graph.getEdgeCountByLabel(edgeLabel, evaluationTimeout);
            } else {
                count = FireflyHelper.countEdges(graph, evaluationTimeout);
            }
            return this.getTraversal().getTraverserGenerator().generate(count, (Step) this, 1L);
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
