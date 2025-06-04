package com.aerospike.firefly.process.computer;

import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Set;

// allows to add parameters to TinkerPop vertex programs
public class VertexProgramConfig implements VertexProgram {

    public static final String TRAVERSAL_VERTEX_PROGRAM_STEP = "gremlin.traversalVertexProgram";
    public static final String OPTIONS = "gremlin.options";

    private final Configuration configuration = new BaseConfiguration();

    public void set(final String key, final Object value) {
        configuration.setProperty(key, value);
    }

    public Object get(final String key) {
        return configuration.getProperty(key);
    }

    public Configuration getConfiguration() {
        return configuration;
    }

    @Override
    public void storeState(final Configuration configuration) {
        ConfigurationUtils.copy(this.configuration, configuration);
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        ConfigurationUtils.copy(configuration, this.configuration);
    }

    @Override
    public void setup(final Memory memory) {
        throw new IllegalStateException();
    }

    @Override
    public void execute(final Vertex vertex, final Messenger messenger, final Memory memory) {
        throw new IllegalStateException();
    }

    @Override
    public boolean terminate(final Memory memory) {
        throw new IllegalStateException();
    }

    @Override
    public Set<MessageScope> getMessageScopes(final Memory memory) {
        throw new IllegalStateException();
    }

    @Override
    public VertexProgram clone() {
        throw new IllegalStateException();
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        throw new IllegalStateException();
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        throw new IllegalStateException();
    }
}
