package com.aerospike.firefly.olap.process;

import com.aerospike.firefly.olap.codec.PeerPressureCodec;
import com.aerospike.firefly.olap.process.packing.DistributedAerospikeConnection;
import com.aerospike.firefly.olap.structure.MutableDetachedVertexProperty;
import com.aerospike.firefly.process.computer.VertexProgramConfig;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.MapHelper;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.javatuples.Pair;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.olap.codec.PeerPressureCodec.getInVertexIds;
import static com.aerospike.firefly.olap.codec.PeerPressureCodec.getOutVertexIds;
import static com.aerospike.firefly.process.computer.VertexProgramConfig.TRAVERSAL_VERTEX_PROGRAM_STEP;
import static org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram.HALTED_TRAVERSERS;

public class PeerPressureProgram extends AlgorithmProgram {

    private static final String CLUSTER = "gremlin.peerPressureVertexProgram.cluster";
    private static final String INITIAL_VOTE_STRENGTH_TRAVERSAL = "gremlin.pageRankVertexProgram.initialVoteStrengthTraversal";
    private static final String PROPERTY = "gremlin.peerPressureVertexProgram.property";
    private static final String MAX_ITERATIONS = "gremlin.peerPressureVertexProgram.maxIterations";
    private static final String DISTRIBUTE_VOTE = "gremlin.peerPressureVertexProgram.distributeVote";
    //private static final String EDGE_TRAVERSAL = "gremlin.peerPressureVertexProgram.edgeTraversal";
    private static final String VOTE_TO_HALT = "gremlin.peerPressureVertexProgram.voteToHalt";

    // vertex properties
    public static final String OUT_VERTICES = "~gremlin.peerPressureVertexProgram.outVertices";
    public static final String IN_VERTICES = "~gremlin.peerPressureVertexProgram.inVertices";
    public static final String VOTE_STRENGTH = "~gremlin.peerPressureVertexProgram.voteStrength";

    // not implemented
    // private PureTraversal<Vertex, Edge> edgeTraversal = null;
    // used only to get the results of the previous vertex program
    private PureTraversal<Vertex, ? extends Number> initialVoteStrengthTraversal = null;
    private int maxIterations = 30;
    // looks like it's not set in TinkerPop
    private boolean distributeVote = false;

    private static final Set<MemoryComputeKey> MEMORY_COMPUTE_KEYS = new HashSet<>(Arrays.asList(
            MemoryComputeKey.of(VOTE_TO_HALT, Operator.and, false, true),
            MemoryComputeKey.of(HALTED_TRAVERSERS, Operator.addAll, false, false),
            MemoryComputeKey.of(START_STEP, Operator.assign, true, false)));

    // for serialization
    private PeerPressureProgram() {
    }

    public PeerPressureProgram(final VertexProgramConfig program, final FireflyGraph graph) {
        final BaseConfiguration configuration = new BaseConfiguration();
        program.storeState(configuration);
        loadState(graph, configuration);
    }

    private void init(final FireflyGraph graph) {
        final Traversal.Admin t = graph.traversal().V().asAdmin();
        t.setStrategies(TraversalStrategies.GlobalCache.getStrategies(graph.getClass()).clone());
        t.getStrategies().addStrategies(optionsStrategy);
        this.graphTraversal = new PureTraversal(t);

        this.columnName = PeerPressureCodec.PEER_PRESSURE_COL;
        this.codec = new PeerPressureCodec(t, this.property);
        this.graph = graph;
        this.db = new DistributedAerospikeConnection(graph);
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        if (configuration.containsKey(INITIAL_VOTE_STRENGTH_TRAVERSAL))
            this.initialVoteStrengthTraversal = PureTraversal.loadState(configuration, INITIAL_VOTE_STRENGTH_TRAVERSAL, graph);
        property = configuration.getString(PROPERTY, CLUSTER);
        this.maxIterations = configuration.getInt(MAX_ITERATIONS, 30);
        this.distributeVote = configuration.getBoolean(DISTRIBUTE_VOTE, false);

        this.vertexProgramTraversal = PureTraversal.loadState(configuration, TRAVERSAL_VERTEX_PROGRAM_STEP, graph);
        this.optionsStrategy = OptionsStrategy.create(configuration);

        init((FireflyGraph) graph);
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
        configuration.setProperty(PROPERTY, property);
        configuration.setProperty(MAX_ITERATIONS, this.maxIterations);
        configuration.setProperty(DISTRIBUTE_VOTE, this.distributeVote);
        if (null != this.initialVoteStrengthTraversal)
            this.initialVoteStrengthTraversal.storeState(configuration, INITIAL_VOTE_STRENGTH_TRAVERSAL);
        if (null != this.vertexProgramTraversal)
            this.vertexProgramTraversal.storeState(configuration, TRAVERSAL_VERTEX_PROGRAM_STEP);
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return MEMORY_COMPUTE_KEYS;
    }

    @Override
    public void setup(final Memory memory) {
        memory.set(VOTE_TO_HALT, false);
    }

    @Override
    public void execute(final BatchJob job, final Memory memory) {
        if (1 == memory.getIteration()) {
            memory.add(VOTE_TO_HALT, false);

            // looks like never happens in TinkerPop
            if (this.distributeVote) {
                job.getStarts().forEach(traverser -> {
                    final DetachedVertex vertex = buildDetached((Vertex) traverser.get());
                    traverser.set(vertex);

                    // initial vote strength
                    final List<String> connectedVertices = vertex.value(IN_VERTICES);
                    connectedVertices.forEach(vertexId ->
                            db.addPackedPair(vertexId, "c", 1.0D, 1)
                    );
                });
            } else {
                job.getStarts().forEach(traverser -> {
                    final DetachedVertex vertex = buildDetached((Vertex) traverser.get());
                    traverser.set(vertex);
                    final double voteStrength = null == this.initialVoteStrengthTraversal ?
                            1.0d :
                            TraversalUtil.apply(vertex, this.initialVoteStrengthTraversal.get()).doubleValue();

                    final MutableDetachedVertexProperty p = (MutableDetachedVertexProperty) vertex.property(property);
                    p.setValue(vertex.id().toString());

                    final MutableDetachedVertexProperty p2 = (MutableDetachedVertexProperty) vertex.property(VOTE_STRENGTH);
                    p2.setValue(voteStrength);

                    final List<String> connectedVertices = vertex.value(OUT_VERTICES);
                    connectedVertices.forEach(vertexId ->
                            db.addPackedPair(vertexId, vertex.id().toString(), voteStrength, 1)
                    );
                });
            }
        } else if (2 == memory.getIteration() && this.distributeVote) {
            updateDetached(job.getStarts(), memory.getIteration());

            job.getStarts().forEach(traverser -> {
                final DetachedVertex vertex = buildDetached((Vertex) traverser.get());
                final String id = graph.getIdFactory().createVertexId(vertex.id()).toString();
                double voteStrength = (null == this.initialVoteStrengthTraversal ?
                        1.0d :
                        TraversalUtil.apply(vertex, this.initialVoteStrengthTraversal.get()).doubleValue()) /
                        IteratorUtils.reduce(IteratorUtils.map(db.getPackedPairs(id, memory.getIteration() - 1), Pair::getValue1), 0.0d, (a, b) -> a + b);

                final MutableDetachedVertexProperty p = (MutableDetachedVertexProperty) vertex.property(property);
                p.setValue(vertex.id().toString());

                final MutableDetachedVertexProperty p2 = (MutableDetachedVertexProperty) vertex.property(VOTE_STRENGTH);
                p2.setValue(voteStrength);

                final List<String> connectedVertices = vertex.value(OUT_VERTICES);
                connectedVertices.forEach(vertexId ->
                        db.addPackedPair(vertexId, vertex.id().toString(), voteStrength, memory.getIteration())
                );
                memory.add(VOTE_TO_HALT, false);
            });
        } else {
            updateDetached(job.getStarts(), memory.getIteration());

            job.getStarts().forEach(traverser -> {
                final DetachedVertex vertex = buildDetached((Vertex) traverser.get());
                final String id = graph.getIdFactory().createVertexId(vertex.id()).toString();
                final Map<String, Double> votes = new HashMap<>();
                votes.put(vertex.value(property), vertex.<Double>value(VOTE_STRENGTH));

                // get pairs vertexId-vote
                db.getPackedPairs(id, memory.getIteration() - 1).forEach(p -> MapHelper.incr(votes, p.getValue0(), p.getValue1()));

                String cluster = largestCount(votes);
                if (null == cluster) cluster = vertex.id().toString();
                memory.add(VOTE_TO_HALT, vertex.value(property).equals(cluster));

                final MutableDetachedVertexProperty p = (MutableDetachedVertexProperty) vertex.property(property);
                p.setValue(cluster);

                // send votes to cluster
                final List<String> connectedVertices = vertex.value(OUT_VERTICES);
                final String finalCluster = cluster;
                connectedVertices.forEach(vertexId ->
                        db.addPackedPair(vertexId, finalCluster, vertex.<Double>value(VOTE_STRENGTH), memory.getIteration())
                );
            });
        }

        job.pass();
    }

    @Override
    public boolean terminate(final Memory memory) {
        final boolean voteToHalt = memory.<Boolean>get(VOTE_TO_HALT)
                || memory.getIteration() >= (this.distributeVote ? this.maxIterations + 1 : this.maxIterations);
        if (voteToHalt) {
            return true;
        } else {
            memory.set(VOTE_TO_HALT, true);
            return false;
        }
    }

    @Override
    public String toString() {
        return StringFactory.vertexProgramString(this, "distributeVote=" + this.distributeVote + ", maxIterations=" + this.maxIterations);
    }

    @Override
    protected DetachedVertex buildDetached(final Vertex vertex) {
        if (vertex instanceof DetachedVertex)
            return (DetachedVertex) vertex;

        final List<String> outVertices = getOutVertexIds(vertex);
        final List<String> inVertices = getInVertexIds(vertex);

        final DetachedVertexProperty outVertexProperty = new DetachedVertexProperty(null, OUT_VERTICES, outVertices, null);
        final DetachedVertexProperty inVertexProperty = new DetachedVertexProperty(null, IN_VERTICES, inVertices, null);
        final MutableDetachedVertexProperty peerPressureProperty = new MutableDetachedVertexProperty(null, property, 0.0, null);
        final MutableDetachedVertexProperty voteStrengthProperty = new MutableDetachedVertexProperty(null, VOTE_STRENGTH, 1.0, null);

        return new DetachedVertex(vertex.id(), "", List.of(outVertexProperty, inVertexProperty, peerPressureProperty, voteStrengthProperty));
    }

    private static <T> T largestCount(final Map<T, Double> map) {
        T largestKey = null;
        double largestValue = Double.MIN_VALUE;
        for (Map.Entry<T, Double> entry : map.entrySet()) {
            if (entry.getValue() == largestValue) {
                if (null != largestKey && largestKey.toString().compareTo(entry.getKey().toString()) > 0) {
                    largestKey = entry.getKey();
                    largestValue = entry.getValue();
                }
            } else if (entry.getValue() > largestValue) {
                largestKey = entry.getKey();
                largestValue = entry.getValue();
            }
        }
        return largestKey;
    }
}
