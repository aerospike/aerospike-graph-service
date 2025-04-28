package com.aerospike.firefly.process.traversal.step.computer;

import com.aerospike.firefly.process.computer.VertexProgramConfig;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.VertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.Configuring;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.List;
import java.util.Set;

import static com.aerospike.firefly.process.computer.VertexProgramConfig.TRAVERSAL_VERTEX_PROGRAM_STEP;


public class VertexProgramProxyStep extends VertexProgramStep implements TraversalParent, Configuring {

    private VertexProgramStep programStep;
    private TraversalVertexProgramStep traversalVertexProgramStep;

    public VertexProgramProxyStep(final VertexProgramStep step, final TraversalVertexProgramStep filterStep) {
        super(step.getTraversal());

        // todo: verify Step is Configuring and TraversalParent
        this.programStep = step;
        this.traversalVertexProgramStep = filterStep;
    }

    @Override
    public void configure(final Object... keyValues) {
        ((Configuring) programStep).configure(keyValues);
    }

    @Override
    public Parameters getParameters() {
        return ((Configuring) programStep).getParameters();
    }

    @Override
    public List<Traversal.Admin<Vertex, Edge>> getLocalChildren() {
        return ((TraversalParent) programStep).getLocalChildren();
    }

    @Override
    public String toString() {
        return StringFactory.stepString(this, programStep.toString());
    }

    @Override
    public VertexProgramConfig generateProgram(final Graph graph, final Memory memory) {
        final VertexProgram program = programStep.generateProgram(graph, memory);

        final Configuration config = new BaseConfiguration();
        program.storeState(config);

        final VertexProgramConfig configProgram = new VertexProgramConfig();
        configProgram.loadState(graph, config);

        traversalVertexProgramStep.computerTraversal.storeState(configProgram.getConfiguration(), TRAVERSAL_VERTEX_PROGRAM_STEP);

        return configProgram;
    }

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return programStep.getRequirements();
    }

    @Override
    public VertexProgramProxyStep clone() {
        final VertexProgramProxyStep clone = (VertexProgramProxyStep) super.clone();
        clone.programStep = (VertexProgramStep) this.programStep.clone();
        clone.traversalVertexProgramStep = this.traversalVertexProgramStep.clone();
        return clone;
    }

    @Override
    public void setTraversal(final Traversal.Admin<?, ?> parentTraversal) {
        programStep.setTraversal(parentTraversal);
    }

    @Override
    public int hashCode() {
        return programStep.hashCode();
    }
}
