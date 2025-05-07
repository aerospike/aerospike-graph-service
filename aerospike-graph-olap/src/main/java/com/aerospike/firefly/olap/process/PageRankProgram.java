package com.aerospike.firefly.olap.process;

import com.aerospike.firefly.olap.codec.Codec;
import com.aerospike.firefly.olap.codec.PageRankCodec;
import com.aerospike.firefly.olap.process.packing.DistributedAerospikeConnection;
import com.aerospike.firefly.olap.structure.MutableDetachedVertexProperty;
import com.aerospike.firefly.process.computer.VertexProgramConfig;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.VertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.aerospike.firefly.olap.codec.PageRankCodec.getOutVertexIds;
import static com.aerospike.firefly.olap.helper.ProgramHelper.executeVertexProgram;
import static com.aerospike.firefly.olap.helper.ProgramHelper.removeTemporaryProperties;
import static com.aerospike.firefly.process.computer.VertexProgramConfig.TRAVERSAL_VERTEX_PROGRAM_STEP;
import static org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram.HALTED_TRAVERSERS;

public class PageRankProgram implements FireflyProgram {

    private static final String ALPHA = "gremlin.pageRankProgram.alpha";
    private static final String MAX_ITERATIONS = "gremlin.pageRankProgram.maxIterations";
    private static final String EDGE_TRAVERSAL = "gremlin.pageRankProgram.edgeTraversal";
    private static final String PROPERTY = "gremlin.pageRankProgram.property";
    private static final String EPSILON = "gremlin.pageRankProgram.epsilon";
    private static final String INITIAL_RANK_TRAVERSAL = "gremlin.pageRankProgram.initialRankTraversal";

    private static final String PAGE_RANK = "gremlin.pageRankProgram.pageRank";
    public static final String OUT_VERTICES = "~gremlin.pageRankProgram.outVertices";

    private static final String VERTEX_COUNT = "vertexCount~L";
    private static final String TELEPORTATION_ENERGY = "teleportationEnergy~D";
    private static final String CONVERGENCE_ERROR = "convergenceError~D";

    // todo: not implemented
    private PureTraversal<Vertex, Edge> edgeTraversal = null;
    private PureTraversal<Vertex, ? extends Number> initialRankTraversal = null;
    private PureTraversal<Vertex, Vertex> graphTraversal;
    private PureTraversal<Vertex, ?> vertexProgramTraversal;

    private double alpha = 0.85d;
    private double epsilon = 0.00001d;
    private int maxIterations = 20;
    public static String property = PAGE_RANK;
    private Set<MemoryComputeKey> memoryComputeKeys;

    private Codec codec;
    private FireflyGraph graph;
    private DistributedAerospikeConnection db;

    // for serialization
    private PageRankProgram() {
    }

    public PageRankProgram(final VertexProgramConfig program, final FireflyGraph graph) {
        final BaseConfiguration configuration = new BaseConfiguration();
        program.storeState(configuration);
        loadState(graph, configuration);

        init(graph);
    }

    private void init(final FireflyGraph graph) {
        final Traversal.Admin t = graph.traversal().V().asAdmin();
        t.setStrategies(TraversalStrategies.GlobalCache.getStrategies(graph.getClass()));
        this.graphTraversal = new PureTraversal(t);

        this.codec = new PageRankCodec(t);
        this.graph = graph;
        this.db = new DistributedAerospikeConnection(graph);
    }

    @Override
    public void execute(final BatchJob job, final Memory memory) {
        if (1 == memory.getIteration()) {
            memory.add(VERTEX_COUNT, (long) job.getStarts().size());

            job.getStarts().forEach(traverser -> {
                final DetachedVertex vertex = buildDetached((Vertex) traverser.get());
                traverser.set(vertex);
            });

            job.pass();

            // CONVERGENCE_ERROR used to determine if we need to finish
            memory.set(CONVERGENCE_ERROR, 1.0d);
            return;
        }

        final double vertexCount = memory.<Long>get(VERTEX_COUNT);

        final double initialRank = null == this.initialRankTraversal ?
                0.0d :
                TraversalUtil.apply(new ReferenceVertex(-1), this.initialRankTraversal.get()).doubleValue();

        final double teleportationEnergy = memory.get(TELEPORTATION_ENERGY);
        job.getStarts().forEach(traverser -> {
            final DetachedVertex vertex = (DetachedVertex) traverser.get();
            final String id = graph.getIdFactory().createVertexId(vertex.id()).toString();
            final List<String> outVertices = vertex.value(OUT_VERTICES);

            double pageRank = 2 == memory.getIteration()
                    ? initialRank
                    : db.getPackedAccumulatorDouble(id, memory.getIteration() - 1);

            //////////////////////////
            if (teleportationEnergy > 0.0d) {
                final double localTerminalEnergy = teleportationEnergy / vertexCount;
                pageRank = pageRank + localTerminalEnergy;
                memory.add(TELEPORTATION_ENERGY, -localTerminalEnergy);
            }
            final double previousPageRank = vertex.<Double>property(property).orElse(0.0d);
            memory.add(CONVERGENCE_ERROR, Math.abs(pageRank - previousPageRank));

            final MutableDetachedVertexProperty p = (MutableDetachedVertexProperty) vertex.properties(property).next();
            p.setValue(pageRank);

            memory.add(TELEPORTATION_ENERGY, (1.0d - this.alpha) * pageRank);
            pageRank = this.alpha * pageRank;

            if (outVertices.isEmpty())
                memory.add(TELEPORTATION_ENERGY, pageRank);
            else {
                final double finalPageRank = pageRank;
                outVertices.forEach(vertexId ->
                        db.addPackedAccumulatorDouble(vertexId, finalPageRank / outVertices.size(), memory.getIteration())
                );
            }
            traverser.set(vertex);
        });

        job.pass();
    }

    private DetachedVertex buildDetached(final Vertex vertex) {
        if (vertex instanceof DetachedVertex)
            return (DetachedVertex) vertex;

        final List<String> outVertices = getOutVertexIds(vertex);

        final DetachedVertexProperty outVertexProperty = new DetachedVertexProperty(null, OUT_VERTICES, outVertices, null);
        final MutableDetachedVertexProperty pagerankProperty = new MutableDetachedVertexProperty(null, property, 0.0, null);

        return new DetachedVertex(vertex.id(), "", List.of(outVertexProperty, pagerankProperty));
    }

    @Override
    public Codec getCodec() {
        return codec;
    }

    @Override
    public PureTraversal<?, ?> getTraversal() {
        return graphTraversal;
    }

    @Override
    public boolean terminate(final Memory memory) {
        boolean terminate = memory.<Double>get(CONVERGENCE_ERROR) < this.epsilon || memory.getIteration() >= this.maxIterations;
        memory.set(CONVERGENCE_ERROR, 0.0d);
        return terminate;
    }

    @Override
    public void setup(final Memory memory) {
        memory.set(TELEPORTATION_ENERGY, null == this.initialRankTraversal ? 1.0d : 0.0d);
        memory.set(VERTEX_COUNT, 0L);
        memory.set(CONVERGENCE_ERROR, 1.0d);
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        if (configuration.containsKey(INITIAL_RANK_TRAVERSAL))
            this.initialRankTraversal = PureTraversal.loadState(configuration, INITIAL_RANK_TRAVERSAL, graph);
        if (configuration.containsKey(EDGE_TRAVERSAL)) {
            this.edgeTraversal = PureTraversal.loadState(configuration, EDGE_TRAVERSAL, graph);
        }
        this.alpha = configuration.getDouble(ALPHA, this.alpha);
        this.epsilon = configuration.getDouble(EPSILON, this.epsilon);
        this.maxIterations = configuration.getInt(MAX_ITERATIONS, 20);
        property = configuration.getString(PROPERTY, PAGE_RANK);
        this.memoryComputeKeys = new HashSet<>(Arrays.asList(
                MemoryComputeKey.of(TELEPORTATION_ENERGY, Operator.sum, true, true),
                MemoryComputeKey.of(VERTEX_COUNT, Operator.sumLong, true, false),
                MemoryComputeKey.of(CONVERGENCE_ERROR, Operator.sum, false, true),
                // todo:
                MemoryComputeKey.of(HALTED_TRAVERSERS, Operator.addAll, false, false)));

        this.vertexProgramTraversal = PureTraversal.loadState(configuration, TRAVERSAL_VERTEX_PROGRAM_STEP, graph);

        init((FireflyGraph) graph);
    }

    @Override
    public void storeState(final Configuration configuration) {
        FireflyProgram.super.storeState(configuration);
        configuration.setProperty(ALPHA, this.alpha);
        configuration.setProperty(EPSILON, this.epsilon);
        configuration.setProperty(PROPERTY, property);
        configuration.setProperty(MAX_ITERATIONS, this.maxIterations);
        if (null != this.edgeTraversal)
            this.edgeTraversal.storeState(configuration, EDGE_TRAVERSAL);
        if (null != this.initialRankTraversal)
            this.initialRankTraversal.storeState(configuration, INITIAL_RANK_TRAVERSAL);
        if (null != this.vertexProgramTraversal)
            this.vertexProgramTraversal.storeState(configuration, TRAVERSAL_VERTEX_PROGRAM_STEP);
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return Collections.emptySet();
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return this.memoryComputeKeys;
    }

    @Override
    public VertexProgram<TraverserSet<Object>> clone() {
        return null;
    }

    @Override
    public GraphComputer.ResultGraph getPreferredResultGraph() {
        return GraphComputer.ResultGraph.ORIGINAL;
    }

    @Override
    public GraphComputer.Persist getPreferredPersist() {
        return GraphComputer.Persist.NOTHING;
    }

    @Override
    public String toString() {
        return StringFactory.vertexProgramString(this, "alpha=" + this.alpha + ", epsilon=" + this.epsilon + ", iterations=" + this.maxIterations);
    }

    @Override
    public Features getFeatures() {
        return new Features() {
            @Override
            public boolean requiresVertexPropertyAddition() {
                return true;
            }
        };
    }

    @Override
    public void postProcessResults(final TraverserSet traversers) {
        removeTemporaryProperties(traversers, property);

        executeVertexProgram(traversers, vertexProgramTraversal);
    }
}
