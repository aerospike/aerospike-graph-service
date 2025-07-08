package com.aerospike.firefly.olap.process;

import com.aerospike.firefly.olap.codec.PageRankCodec;
import com.aerospike.firefly.olap.helper.TaskLogger;
import com.aerospike.firefly.olap.helper.TimeLog;
import com.aerospike.firefly.olap.process.packing.ByteArrayWrapper;
import com.aerospike.firefly.olap.process.packing.DistributedAerospikeConnection;
import com.aerospike.firefly.olap.structure.AerospikeComputeKey;
import com.aerospike.firefly.olap.structure.MutableDetachedVertexProperty;
import com.aerospike.firefly.process.computer.VertexProgramConfig;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.commons.configuration2.BaseConfiguration;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.ranking.pagerank.PageRankVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalUtil;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceVertex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static com.aerospike.firefly.olap.codec.PageRankCodec.getOutVertexCount;
import static com.aerospike.firefly.process.computer.VertexProgramConfig.TRAVERSAL_VERTEX_PROGRAM_STEP;
import static org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram.HALTED_TRAVERSERS;

public class PageRankProgram extends AlgorithmProgram {
    private static final Logger LOGGER = LoggerFactory.getLogger(PageRankProgram.class);

    private static final int BULK_WRITE_SIZE = 100;

    // config constants, same as in TinkerPop
    private static final String ALPHA = "gremlin.pageRankVertexProgram.alpha";
    private static final String EPSILON = "gremlin.pageRankVertexProgram.epsilon";
    private static final String MAX_ITERATIONS = "gremlin.pageRankVertexProgram.maxIterations";
    private static final String EDGE_TRAVERSAL = "gremlin.pageRankVertexProgram.edgeTraversal";
    private static final String INITIAL_RANK_TRAVERSAL = "gremlin.pageRankVertexProgram.initialRankTraversal";
    private static final String PROPERTY = "gremlin.pageRankVertexProgram.property";

    private static final String SAVE_RESULTS = "gremlin.pageRankVertexProgram.saveResults";

    // vertex properties
    public static final String IN_VERTICES = "~gremlin.pageRankProgram.inVertices";
    public static final String OUT_VERTEX_COUNT = "~gremlin.pageRankProgram.outVertexCount";

    // memory keys
    private static final String VERTEX_COUNT = AerospikeComputeKey.createLong("vertexCount", false);
    private static final String TELEPORTATION_ENERGY = AerospikeComputeKey.createDouble("teleportationEnergy", true);
    private static final String CONVERGENCE_ERROR = AerospikeComputeKey.createDouble("convergenceError", true);

    // VOTE_TO_HALT - end of program on this worker.
    // VOTE_TO_SAVE_RESULTS - end of computation on this worker, time to save results. Next iteration will be VOTE_TO_HALT.
    private static final String VOTE_TO_SAVE_RESULTS = "gremlin.traversalVertexProgram.voteToSaveResults";

    // todo: not implemented
    private PureTraversal<Vertex, Edge> edgeTraversal = null;
    private PureTraversal<Vertex, ? extends Number> initialRankTraversal = null;

    private double alpha = 0.85d;
    private double epsilon = 0.00001d;
    private int maxIterations = 20;
    private Set<MemoryComputeKey> memoryComputeKeys;

    // for serialization
    private PageRankProgram() {
    }

    public PageRankProgram(final VertexProgramConfig program, final FireflyGraph graph) {
        final BaseConfiguration configuration = new BaseConfiguration();
        program.storeState(configuration);
        loadState(graph, configuration);
    }

    private void init(final FireflyGraph graph) {
        final Traversal.Admin t = graph.traversal().V().asAdmin();
        t.setStrategies(TraversalStrategies.GlobalCache.getStrategies(graph.getClass()).clone());
        t.getStrategies().addStrategies(optionsStrategy);
        this.graphTraversal = new PureTraversal(t);

        this.columnName = PageRankCodec.PAGERANK_COL;
        this.codec = new PageRankCodec(t, this.property);
        this.graph = graph;
        this.db = new DistributedAerospikeConnection(graph);
    }

    @Override
    public void execute(final BatchJob job, final Memory memory) {
        final String info = String.format("Starting PageRankProgram Iteration %d with %d traversers", memory.getIteration(), job.getStarts().size());
        TaskLogger.logDebuggingMessage(info, LOGGER);

        TimeLog.complete("PageRankProgram.start");
        if (memory.<Boolean>get(VOTE_TO_SAVE_RESULTS)) {
            final List<Object> vertexIds = new ArrayList<>();
            job.getStarts().forEach(traverser -> {
                final DetachedVertex vertex = (DetachedVertex) traverser.get();
                vertexIds.add(vertex.id());
            });

            final Map<FireflyId, Double> valuesToWrite = new HashMap<>();

            job.getStarts().forEach(traverser -> {
                final DetachedVertex vertex = (DetachedVertex) traverser.get();
                valuesToWrite.put(graph.getIdFactory().createVertexId(vertex.id()), vertex.<Double>property(property).value());
                if (valuesToWrite.size() >= BULK_WRITE_SIZE) {
                    db.setProperty(valuesToWrite, property);
                    valuesToWrite.clear();
                }
            });
            // write leftovers for last BatchJob
            db.setProperty(valuesToWrite, property);
            valuesToWrite.clear();

            // results might be filtered and returned
            job.pass();
            TimeLog.complete("PageRankProgram.results saved to db");
            return;
        }


        if (1 == memory.getIteration()) {
            memory.add(VERTEX_COUNT, (long) job.getStarts().size());

            job.getStarts().forEach(traverser -> {
                final DetachedVertex vertex = buildDetached((Vertex) traverser.get());
                traverser.set(vertex);
                TimeLog.complete("PageRankProgram.buildDetached");
            });

            job.pass();
            TimeLog.complete("PageRankProgram.iteration1job.pass()");
            return;
        }

        final double vertexCount = memory.<Long>get(VERTEX_COUNT);

        final double initialRank = null == this.initialRankTraversal ?
                0.0d :
                TraversalUtil.apply(new ReferenceVertex(-1), this.initialRankTraversal.get()).doubleValue();

        updateDetached(job.getStarts(), memory.getIteration());

        TimeLog.complete("PageRankProgram.updateDetached");
        // should never happen
        final AtomicReference<FireflyVertex> anyFireflyVertex = new AtomicReference<>();
        job.getStarts().forEach(traverser -> {
            if (traverser.get() instanceof FireflyVertex) {
                anyFireflyVertex.set((FireflyVertex) traverser.get());
                final DetachedVertex vertex = buildDetached((Vertex) traverser.get());
                traverser.set(vertex);
            }
        });
        if (anyFireflyVertex.get() != null) {
            TaskLogger.logDebuggingMessage("Possible spark recovery started, found FireflyVertex " + anyFireflyVertex.get() + " at iteration " + memory.getIteration(), LOGGER);
        }

        // prepare cache

        TimeLog.complete("PageRankProgram.setJobStarts");
        final Map<ByteArrayWrapper, Double> cache;
        if (memory.getIteration() > 2) {
            final Set<ByteArrayWrapper> ids = new HashSet<>();
            job.getStarts().forEach(traverser -> {
                final DetachedVertex vertex = (DetachedVertex) traverser.get();
                final List<byte[]> inVids = vertex.value(IN_VERTICES);
                for (byte[] inVid : inVids) {
                    ids.add(new ByteArrayWrapper(inVid));
                }
            });
            TimeLog.complete("PageRankProgram.creatingIdSet(iteration>2)");
            cache = db.getPackedAccumulatorDoubleCache(new ArrayList<>(ids), memory.getIteration() - 1);
            ids.clear();
        } else {
            cache = Collections.emptyMap();
        }
        TimeLog.complete("PageRankProgram.getCache(iteration>2)");

        final double teleportationEnergy = memory.get(TELEPORTATION_ENERGY);
        final Map<byte[], Double> writeBatch = new HashMap<>();
        job.getStarts().forEach(traverser -> {
            final DetachedVertex vertex = (DetachedVertex) traverser.get();
            final byte[] id = graph.getIdFactory().createVertexId(vertex.id()).getKeyHash();
            final List<byte[]> inVertices = vertex.value(IN_VERTICES);
            final Long outVertexCount = vertex.value(OUT_VERTEX_COUNT);

            double pageRank = 2 == memory.getIteration()
                    ? initialRank
                    : inVertices.stream()
                    .mapToDouble(vertexId -> cache.getOrDefault(new ByteArrayWrapper(vertexId), 0.0))
                    .sum();

            //////////////////////////
            if (teleportationEnergy > 0.0d) {
                final double localTerminalEnergy = teleportationEnergy / vertexCount;
                pageRank = pageRank + localTerminalEnergy;
                memory.add(TELEPORTATION_ENERGY, -localTerminalEnergy);
            }

            final MutableDetachedVertexProperty p = (MutableDetachedVertexProperty) vertex.property(property);
            final double previousPageRank = (double) p.value();
            memory.add(CONVERGENCE_ERROR, Math.abs(pageRank - previousPageRank));
            p.setValue(pageRank);

            memory.add(TELEPORTATION_ENERGY, (1.0d - this.alpha) * pageRank);
            pageRank = this.alpha * pageRank;

            if (outVertexCount == 0)
                memory.add(TELEPORTATION_ENERGY, pageRank);
            else {
                writeBatch.put(id, pageRank / outVertexCount);
                if (writeBatch.size() >= BULK_WRITE_SIZE) {
                    db.setPackedAccumulatorDouble(writeBatch, memory.getIteration());
                    writeBatch.clear();
                }
            }
            traverser.set(vertex);
        });
        TimeLog.complete("PageRankProgram.writeBatch");

        db.setPackedAccumulatorDouble(writeBatch, memory.getIteration());
        writeBatch.clear();
        cache.clear();

        job.pass();
        TimeLog.complete("PageRankProgram.writeRemainderAndClear");
    }

    @Override
    protected DetachedVertex buildDetached(final Vertex vertex) {
        if (vertex instanceof DetachedVertex)
            return (DetachedVertex) vertex;

        TimeLog.complete("PageRankProgram.buildDetached");
        final List<byte[]> inVertices = ((FireflyVertex)vertex).getConvertedVertexIds(Direction.IN);//getInVertexIds(vertex);
        TimeLog.complete("PageRankProgram.getInVIds");
        final Long outVertexCount = getOutVertexCount(vertex);
        TimeLog.complete("PageRankProgram.getOutVCount");

        final DetachedVertexProperty inVertexProperty = new DetachedVertexProperty(null, IN_VERTICES, inVertices, null);
        final DetachedVertexProperty outVertexCountProperty = new DetachedVertexProperty(null, OUT_VERTEX_COUNT, outVertexCount, null);
        final MutableDetachedVertexProperty pagerankProperty = new MutableDetachedVertexProperty(null, property, 0.0, null);
        TimeLog.complete("PageRankProgram.createProperties");

        return new DetachedVertex(vertex.id(), "", List.of(inVertexProperty, outVertexCountProperty, pagerankProperty));
    }

    @Override
    public boolean terminate(final Memory memory) {
        // we already saved results, time to terminate
        if (memory.<Boolean>get(VOTE_TO_SAVE_RESULTS)) {
            return true;
        }

        boolean terminate = memory.getIteration() >= this.maxIterations
                // first iteration is setup, so no CONVERGENCE_ERROR
                || (memory.getIteration() > 1 && memory.<Double>get(CONVERGENCE_ERROR) < this.epsilon);
        // set CONVERGENCE_ERROR for next iteration
        memory.set(CONVERGENCE_ERROR, 0.0d);

        // additional iteration to save results to db
        if (terminate && (Boolean) optionsStrategy.getOptions().getOrDefault(SAVE_RESULTS, false)
                && !memory.<Boolean>get(VOTE_TO_SAVE_RESULTS)) {
            memory.set(VOTE_TO_SAVE_RESULTS, true);
            return false;
        }

        memory.set(VOTE_TO_SAVE_RESULTS, false);
        return terminate;
    }

    @Override
    public void setup(final Memory memory) {
        memory.set(TELEPORTATION_ENERGY, null == this.initialRankTraversal ? 1.0d : 0.0d);
        memory.set(VERTEX_COUNT, 0L);
        // CONVERGENCE_ERROR used to determine if we need to finish
        memory.set(CONVERGENCE_ERROR, 1.00d);
        memory.set(VOTE_TO_SAVE_RESULTS, false);
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
        this.property = configuration.getString(PROPERTY, PageRankVertexProgram.PAGE_RANK);
        this.memoryComputeKeys = new HashSet<>(Arrays.asList(
                MemoryComputeKey.of(TELEPORTATION_ENERGY, Operator.sum, true, true),
                MemoryComputeKey.of(VERTEX_COUNT, Operator.sumLong, true, false),
                MemoryComputeKey.of(CONVERGENCE_ERROR, Operator.sum, false, true),
                MemoryComputeKey.of(START_STEP, Operator.assign, true, false),
                MemoryComputeKey.of(VOTE_TO_SAVE_RESULTS, Operator.and, true, false),
                // todo:
                MemoryComputeKey.of(HALTED_TRAVERSERS, Operator.addAll, false, false)));

        this.vertexProgramTraversal = PureTraversal.loadState(configuration, TRAVERSAL_VERTEX_PROGRAM_STEP, graph);
        this.optionsStrategy = OptionsStrategy.create(configuration);

        init((FireflyGraph) graph);
    }

    @Override
    public void storeState(final Configuration configuration) {
        super.storeState(configuration);
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
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return this.memoryComputeKeys;
    }

    @Override
    public String toString() {
        return StringFactory.vertexProgramString(this, "alpha=" + this.alpha + ", epsilon=" + this.epsilon + ", iterations=" + this.maxIterations);
    }
}
