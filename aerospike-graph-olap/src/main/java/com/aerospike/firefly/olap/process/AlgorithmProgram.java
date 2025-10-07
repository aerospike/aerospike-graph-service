package com.aerospike.firefly.olap.process;

import com.aerospike.firefly.olap.codec.Codec;
import com.aerospike.firefly.olap.config.QueryParameters;
import com.aerospike.firefly.olap.helper.TaskLogger;
import com.aerospike.firefly.olap.process.packing.DistributedAerospikeConnection;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.commons.configuration2.Configuration;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.lambda.ValueTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.OptionsStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.detached.DetachedVertex;
import org.javatuples.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static com.aerospike.firefly.olap.codec.Codec.ELEMENT_ID_COL;
import static com.aerospike.firefly.olap.helper.ProgramHelper.executeVertexProgram;
import static com.aerospike.firefly.olap.helper.ProgramHelper.getStartStep;
import static com.aerospike.firefly.olap.helper.ProgramHelper.removeTemporaryProperties;
import static com.aerospike.firefly.olap.process.packing.DistributedAerospikeConnection.WRITE_PAGE_SIZE;
import static org.apache.tinkerpop.gremlin.process.traversal.Traverser.Admin.HALT;

public abstract class AlgorithmProgram implements FireflyProgram {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlgorithmProgram.class);

    public static final String START_STEP = "gremlin.algorithmProgram.startStep";

    // VOTE_TO_HALT - end of program on this worker.
    // VOTE_TO_SAVE_RESULTS - end of computation on this worker, time to save results. Next iteration will be VOTE_TO_HALT.
    protected static final String VOTE_TO_SAVE_RESULTS = "gremlin.algorithmProgram.voteToSaveResults";

    private static final Map<String, BiFunction<Column, Number, Column>> predicateToSpark = new HashMap<>() {{
        put("eq", Column::equalTo);
        put("neq", Column::notEqual);
        put("gt", Column::gt);
        put("gte", Column::geq);
        put("lt", Column::lt);
        put("lte", Column::leq);
    }};

    // default is __.outE()
    protected PureTraversal<Vertex, Edge> edgeTraversal = null;
    protected PureTraversal<Vertex, Vertex> graphTraversal;
    protected PureTraversal<Vertex, ?> vertexProgramTraversal;
    protected String property;
    protected String columnName;

    protected OptionsStrategy optionsStrategy;
    protected Codec codec;
    protected FireflyGraph graph;
    protected DistributedAerospikeConnection db;

    protected String jobId;
    private Boolean truncateOlapTempSetAfterExecution = false;

    @Override
    public void postProcessResults(final TraverserSet traversers, final Memory memory) {
        removeTemporaryProperties(traversers, property);

        executeVertexProgram(traversers, vertexProgramTraversal, getStartStep(memory, START_STEP, vertexProgramTraversal.get()));
    }

    @Override
    public boolean validPostProcessSteps() {
        if ((Boolean) optionsStrategy.getOptions().getOrDefault(QueryParameters.ALLOW_UNFILTERED_ALGORITHM, false))
            return true;

        final Traversal.Admin traversal = vertexProgramTraversal.get();
        final List<Step> steps = traversal.getSteps();

        if (steps.isEmpty() || !(steps.get(0) instanceof GraphStep)) {
            return false;
        }

        final GraphStep graphStep = (GraphStep) steps.get(0);
        if (graphStep.getIds().length != 0) {
            return true;
        }

        if (steps.size() == 1) {
            return false;
        }

        if (steps.get(1) instanceof OrderGlobalStep) {
            final OrderGlobalStep orderStep = (OrderGlobalStep) steps.get(1);
            final List<Pair<Traversal.Admin, Comparator>> comparators = orderStep.getComparators();
            if (comparators.size() == 1
                    && !comparators.get(0).getValue1().equals(Order.shuffle)
                    && comparators.get(0).getValue0() instanceof ValueTraversal
                    && ((ValueTraversal) comparators.get(0).getValue0()).getPropertyKey().equals(property)
                    // only order followed by limit supported
                    && steps.size() > 2 && steps.get(2) instanceof RangeGlobalStep
                    && ((RangeGlobalStep) steps.get(2)).getLowRange() == 0) {
                return true;
            }
        }

        if (steps.get(1) instanceof HasStep) {
            final HasStep hasStep = (HasStep) steps.get(1);
            final List<HasContainer> hasContainers = hasStep.getHasContainers();
            if (hasContainers.size() == 1 && hasContainers.get(0).getKey().equals(property)) {
                final String predicateName = hasContainers.get(0).getPredicate().getBiPredicate().getPredicateName();
                return predicateToSpark.containsKey(predicateName);
            }
        }

        return false;
    }

    protected void saveResultAsProperty(final BatchJob job) {
        final Map<FireflyId, Object> valuesToWrite = new HashMap<>();

        job.getStarts().forEach(traverser -> {
            final DetachedVertex vertex = (DetachedVertex) traverser.get();
            valuesToWrite.put(graph.getIdFactory().createVertexId(vertex.id()), vertex.property(property).value());
            if (valuesToWrite.size() >= WRITE_PAGE_SIZE) {
                db.setProperty(valuesToWrite, property);
                valuesToWrite.clear();
            }
        });
        // write leftovers for last BatchJob
        db.setProperty(valuesToWrite, property);
        valuesToWrite.clear();

        // results might be filtered and returned
        job.pass();
    }

    @Override
    public void storeState(final Configuration configuration) {
        optionsStrategy.getOptions().forEach(configuration::addProperty);
        FireflyProgram.super.storeState(configuration);
    }

    @Override
    public Pair<Boolean, Dataset<Row>> postProcessResults(final Dataset<Row> dataset, final Memory memory) {
        final Traversal.Admin traversal = vertexProgramTraversal.get();
        final List<Step> steps = traversal.getSteps();

        if (steps.isEmpty() || !(steps.get(0) instanceof GraphStep)) {
            memory.set(START_STEP, HALT);
            return Pair.with(false, null);
        }

        memory.set(START_STEP, steps.get(0).getId());

        final GraphStep graphStep = (GraphStep) steps.get(0);
        memory.set(START_STEP, graphStep.getId());
        if (graphStep.getIds().length != 0) {
            // todo: type handling? integer ids vs long ids (!!!)
            final Dataset<Row> result = dataset.filter(dataset.col(ELEMENT_ID_COL).isin(graphStep.getIds()));
            TaskLogger.logDebuggingMessage("== doing native filtering ==", LOGGER);
            return Pair.with(true, result);
        }

        if (steps.size() == 1) {
            return Pair.with(false, null);
        }

        if (steps.get(1) instanceof OrderGlobalStep) {
            final OrderGlobalStep orderStep = (OrderGlobalStep) steps.get(1);
            final List<Pair<Traversal.Admin, Comparator>> comparators = orderStep.getComparators();
            if (comparators.size() == 1
                    && !comparators.get(0).getValue1().equals(Order.shuffle)
                    && comparators.get(0).getValue0() instanceof ValueTraversal
                    && ((ValueTraversal) comparators.get(0).getValue0()).getPropertyKey().equals(property)) {

                memory.set(START_STEP, orderStep.getNextStep().getId());

                // do we have the following limit step?
                int limit = -1;
                if (steps.size() > 2 && steps.get(2) instanceof RangeGlobalStep) {
                    final RangeGlobalStep rangeGlobalStep = (RangeGlobalStep) steps.get(2);
                    if (rangeGlobalStep.getLowRange() == 0) {
                        limit = (int) rangeGlobalStep.getHighRange();
                        memory.set(START_STEP, rangeGlobalStep.getNextStep().getId());
                    }
                }

                final Column sortColumn = comparators.get(0).getValue1().equals(Order.asc)
                        ? new Column(columnName)
                        : new Column(columnName).desc();
                final Dataset<Row> result = limit == -1 ? dataset.orderBy(sortColumn) : dataset.orderBy(sortColumn).limit(limit);
                TaskLogger.logDebuggingMessage(limit == -1
                        ? "== doing native sorting by property =="
                        : "== doing native sorting by property and limiting ==", LOGGER);

                return Pair.with(true, result);
            }
        }

        if (steps.get(1) instanceof HasStep) {
            final HasStep hasStep = (HasStep) steps.get(1);
            final List<HasContainer> hasContainers = hasStep.getHasContainers();
            if (hasContainers.size() == 1 && hasContainers.get(0).getKey().equals(property)) {
                final String predicateName = hasContainers.get(0).getPredicate().getBiPredicate().getPredicateName();
                if (predicateToSpark.containsKey(predicateName)) {
                    final Column filterColumn = predicateToSpark.get(predicateName)
                            .apply(new Column(columnName), (Number) hasContainers.get(0).getPredicate().getValue());
                    final Dataset<Row> result = dataset.filter(filterColumn);
                    memory.set(START_STEP, hasStep.getNextStep().getId());

                    TaskLogger.logDebuggingMessage("== doing native filtering by property ==", LOGGER);

                    return Pair.with(true, result);
                } else {
                    LOGGER.warn("Predicate " + predicateName + " translation to spark is not supported.");
                }
            }
        }

        return Pair.with(false, null);
    }

    @Override
    public Codec getCodec() {
        return codec;
    }

    @Override
    public PureTraversal<?, ?> getTraversal() {
        return graphTraversal;
    }

    protected void setTraversal() {
        final Traversal.Admin t = graph.traversal().V().asAdmin();
        t.setStrategies(TraversalStrategies.GlobalCache.getStrategies(graph.getClass()).clone());
        t.getStrategies().addStrategies(optionsStrategy);
        this.graphTraversal = new PureTraversal(t);
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
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return Collections.emptySet();
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
    public AlgorithmProgram clone() {
        return null;
    }

    protected abstract DetachedVertex buildDetached(final Vertex vertex);

    /*
     * This function is only needed in case of spark recovery.
     */
    protected void updateDetached(final TraverserSet<Object> vertices, final int iteration) {
        // should never happen
        final AtomicReference<FireflyVertex> anyFireflyVertex = new AtomicReference<>();
        vertices.forEach(traverser -> {
            if (traverser.get() instanceof FireflyVertex) {
                anyFireflyVertex.set((FireflyVertex) traverser.get());
                final DetachedVertex vertex = buildDetached((Vertex) traverser.get());
                traverser.set(vertex);
            }
        });
        if (anyFireflyVertex.get() != null) {
            TaskLogger.logDebuggingMessage("Possible spark recovery started, found FireflyVertex "
                    + anyFireflyVertex.get() + " at iteration " + iteration, LOGGER);
        }
    }

    @Override
    public void setJobId(final String jobId) {
        this.jobId = jobId;
    }

    @Override
    public void initDB() {
        db.truncateOlapTempSet();
        truncateOlapTempSetAfterExecution = true;
    }

    @Override
    public void cleanUpDB() {
        if (truncateOlapTempSetAfterExecution) {
            db.truncateOlapTempSet();
            System.out.println("Aerospike work set truncated by job " + jobId);
        }
    }

    protected VertexStep getEdgeTraversalStep() {
        final List<Step> steps = edgeTraversal.get().getSteps();
        return (VertexStep) steps.get(0);
    }

    protected void validateEdgeTraversal() {
        // should never happen, as Tinkerpop PageRankVertexProgram set a default
        if (null == this.edgeTraversal) {
            throw new IllegalStateException("The edge traversal was not set.");
        }
        final List<Step> steps = this.edgeTraversal.get().getSteps();
        if (steps.size() != 1 || !(steps.get(0) instanceof VertexStep) || !((VertexStep) steps.get(0)).returnsEdge()) {
            throw new IllegalStateException("The edge traversal must have only single inE()/outE()/bothE() step.");
        }
    }
}
