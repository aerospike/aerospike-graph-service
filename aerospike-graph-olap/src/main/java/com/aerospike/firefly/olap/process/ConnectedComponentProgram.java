package com.aerospike.firefly.olap.process;

import com.aerospike.firefly.olap.codec.ConnectedComponentCodec;
import com.aerospike.firefly.olap.process.packing.DistributedAerospikeConnection;
import com.aerospike.firefly.olap.structure.MutableDetachedVertexProperty;
import com.aerospike.firefly.process.computer.VertexProgramConfig;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ConfigurationUtils;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.clustering.connected.ConnectedComponentVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.aerospike.firefly.olap.codec.ConnectedComponentCodec.connectedVertexIds;
import static com.aerospike.firefly.process.computer.VertexProgramConfig.TRAVERSAL_VERTEX_PROGRAM_STEP;
import static org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram.HALTED_TRAVERSERS;

public class ConnectedComponentProgram extends AlgorithmProgram {

    private static final String PROPERTY = "gremlin.connectedComponentVertexProgram.property";
    // it's not mistake, same in TinkerPop
    private static final String EDGE_TRAVERSAL = "gremlin.pageRankVertexProgram.edgeTraversal";
    private static final String VOTE_TO_HALT = "gremlin.connectedComponentProgram.voteToHalt";

    // vertex properties
    public static final String CONNECTED_VERTICES = "~gremlin.connectedComponentProgram.connectedVertices";

    private static final Set<MemoryComputeKey> MEMORY_COMPUTE_KEYS = new HashSet<>(Arrays.asList(
            MemoryComputeKey.of(VOTE_TO_HALT, Operator.and, false, true),
            MemoryComputeKey.of(HALTED_TRAVERSERS, Operator.addAll, false, false),
            MemoryComputeKey.of(START_STEP, Operator.assign, true, false)));

    // todo: not implemented
    private PureTraversal<Vertex, Edge> edgeTraversal = null;
    private Configuration configuration;

    // for serialization
    private ConnectedComponentProgram() {
    }

    public ConnectedComponentProgram(final VertexProgramConfig program, final FireflyGraph graph) {
        final BaseConfiguration configuration = new BaseConfiguration();
        program.storeState(configuration);
        loadState(graph, configuration);
    }

    private void init(final FireflyGraph graph) {
        this.optionsStrategy = OptionsStrategy.create(configuration);
        setTraversal(graph);

        this.columnName = ConnectedComponentCodec.COMPONENT_COL;
        this.codec = new ConnectedComponentCodec(this.graphTraversal.get(), this.property);
        this.graph = graph;
        this.db = new DistributedAerospikeConnection(graph, true);
    }

    @Override
    public void execute(final BatchJob job, final Memory memory) {
        if (1 == memory.getIteration()) {
            memory.add(VOTE_TO_HALT, false);

            job.getStarts().forEach(traverser -> {
                final DetachedVertex vertex = buildDetached((Vertex) traverser.get());
                traverser.set(vertex);

                final List<String> connectedVertices = vertex.value(CONNECTED_VERTICES);
                connectedVertices.forEach(vertexId ->
                        this.db.setPackedMinValue(vertexId, vertex.id().toString())
                );
            });

            job.pass();
            return;
        }

        updateDetached(job.getStarts(), memory.getIteration());

        job.getStarts().forEach(traverser -> {
            final DetachedVertex vertex = (DetachedVertex) traverser.get();
            final String id = graph.getIdFactory().createVertexId(vertex.id()).toString();
            final List<String> connectedVertices = vertex.value(CONNECTED_VERTICES);
            String currentComponent = vertex.value(property);

            boolean different = false;
            final String candidate = db.getPackedMinValue(id);
            if (candidate != null && candidate.compareTo(currentComponent) < 0) {
                currentComponent = candidate;
                different = true;
            }

            if (different) {
                final MutableDetachedVertexProperty p = (MutableDetachedVertexProperty) vertex.property(this.property);
                p.setValue(currentComponent);

                final String finalComponent = currentComponent;
                connectedVertices.forEach(vertexId ->
                        this.db.setPackedMinValue(vertexId, finalComponent)
                );

                memory.add(VOTE_TO_HALT, false);
            }
        });

        job.pass();
    }

    @Override
    protected DetachedVertex buildDetached(final Vertex vertex) {
        if (vertex instanceof DetachedVertex)
            return (DetachedVertex) vertex;

        final List<String> connectedVertices = connectedVertexIds(vertex);

        final DetachedVertexProperty connectedVertexProperty = new DetachedVertexProperty(null, CONNECTED_VERTICES, connectedVertices, null);
        final MutableDetachedVertexProperty componentProperty = new MutableDetachedVertexProperty(null, property, vertex.id().toString(), null);

        return new DetachedVertex(vertex.id(), "", List.of(connectedVertexProperty, componentProperty));
    }

    @Override
    public boolean terminate(final Memory memory) {
        final boolean voteToHalt = memory.<Boolean>get(VOTE_TO_HALT);
        if (voteToHalt) {
            return true;
        } else {
            memory.set(VOTE_TO_HALT, true);
            return false;
        }
    }

    @Override
    public void setup(final Memory memory) {
        memory.set(VOTE_TO_HALT, true);
    }

    @Override
    public void loadState(final Graph graph, final Configuration config) {
        configuration = new BaseConfiguration();
        if (config != null) {
            ConfigurationUtils.copy(config, configuration);
        }

        if (configuration.containsKey(EDGE_TRAVERSAL)) {
            this.edgeTraversal = PureTraversal.loadState(configuration, EDGE_TRAVERSAL, graph);
        }
        this.vertexProgramTraversal = PureTraversal.loadState(configuration, TRAVERSAL_VERTEX_PROGRAM_STEP, graph);

        property = configuration.getString(PROPERTY, ConnectedComponentVertexProgram.COMPONENT);

        init((FireflyGraph) graph);
    }

    @Override
    public void storeState(final Configuration config) {
        if (configuration != null) {
            ConfigurationUtils.copy(configuration, config);
        }
        super.storeState(config);
        if (null != this.vertexProgramTraversal)
            this.vertexProgramTraversal.storeState(config, TRAVERSAL_VERTEX_PROGRAM_STEP);
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
    }
}
