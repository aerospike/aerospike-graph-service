/*
 * Copyright 2022-2026 Aerospike, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.aerospike.firefly.olap.process;

import com.aerospike.firefly.olap.codec.Codec;
import com.aerospike.firefly.olap.codec.TraverserCodec;
import com.aerospike.firefly.olap.process.packing.DistributedAerospikeConnection;
import com.aerospike.firefly.olap.process.traversal.step.SparkOperation;
import com.aerospike.firefly.olap.structure.AerospikeComputeKey;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ReflectionHelper;
import org.apache.commons.configuration2.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.Computer;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.computer.MapReduce;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MemoryComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.MessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.ProgramPhase;
import org.apache.tinkerpop.gremlin.process.computer.VertexComputeKey;
import org.apache.tinkerpop.gremlin.process.computer.traversal.MemoryTraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgramMessageCombiner;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.decoration.VertexProgramStrategy;
import org.apache.tinkerpop.gremlin.process.computer.traversal.strategy.finalization.ComputerFinalizationStrategy;
import org.apache.tinkerpop.gremlin.process.computer.util.AbstractVertexProgramBuilder;
import org.apache.tinkerpop.gremlin.process.computer.util.VertexProgramHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSource;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.MapReducer;
import org.apache.tinkerpop.gremlin.process.traversal.step.MemoryComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.IndexedTraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.MutableMetrics;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.ScriptTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMetrics;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.util.function.MutableMetricsSupplier;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram.ACTIVE_TRAVERSERS;
import static org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram.HALTED_TRAVERSERS;

public class TraversalProgram implements FireflyProgram {

    private static final String TRAVERSAL = "gremlin.traversalVertexProgram.traversal";
    public static final String MUTATED_MEMORY_KEYS = "gremlin.traversalVertexProgram.mutatedMemoryKeys";
    public static final String VOTE_TO_HALT = "gremlin.traversalVertexProgram.voteToHalt";
    private static final String COMPLETED_BARRIERS = "gremlin.traversalVertexProgram.completedBarriers";

    private Set<MemoryComputeKey> memoryComputeKeys = new HashSet<>();
    private static final Set<VertexComputeKey> VERTEX_COMPUTE_KEYS =
            new HashSet<>(Arrays.asList(VertexComputeKey.of(HALTED_TRAVERSERS, false), VertexComputeKey.of(ACTIVE_TRAVERSERS, true)));

    public static final String SPARK_FLAG = "gremlin.traversalVertexProgram.sparkFlag";
    private static final MemoryComputeKey SPARK_KEY = MemoryComputeKey.of(SPARK_FLAG, Operator.or, false, true);

    private PureTraversal<?, ?> traversal;
    private TraversalMatrix<?, ?> traversalMatrix;
    private Codec codec;
    private final Set<MapReduce> mapReducers = new HashSet<>();
    private TraverserSet<Object> haltedTraversers;
    // true for last or single Program
    private boolean returnHaltedTraversers = false;
    private boolean profile = false;
    // handle current profile metrics if profile is true
    private MutableMetrics iterationMetrics;
    private DistributedAerospikeConnection db;

    private static final Logger LOGGER = LoggerFactory.getLogger(TraversalProgram.class);
    private String jobId;

    private TraversalProgram() {
    }

    // todo: temporary hack
    public TraversalProgram(final TraversalVertexProgram traversalVertexProgram) {
        this.traversal = traversalVertexProgram.getTraversal();
        this.codec = new TraverserCodec(this.traversal.get());
        this.traversalMatrix = new TraversalMatrix<>(this.traversal.get());

        // used only when more than 1 VertexProgram
        this.haltedTraversers = (TraverserSet<Object>) ReflectionHelper.getFieldValue(traversalVertexProgram, "haltedTraversers");
        this.returnHaltedTraversers = (boolean) ReflectionHelper.getFieldValue(traversalVertexProgram, "returnHaltedTraversers");

        init();

        // does the traversal need profile information
        this.profile = !TraversalHelper.getStepsOfAssignableClassRecursively(ProfileStep.class, this.traversal.get()).isEmpty();
        this.db = new DistributedAerospikeConnection(((FireflyGraph) traversalMatrix.getTraversal().getGraph().get()), jobId);
    }

    // register TraversalVertexProgram specific memory compute keys
    private void init() {
        final Iterator<?> itty = IteratorUtils.filter(this.traversal.get().getStrategies(), strategy -> strategy instanceof HaltedTraverserStrategy).iterator();
        if (itty.hasNext()) {
            throw new IllegalArgumentException("Custom HaltedTraverserStrategy is not supported");
        }

        this.memoryComputeKeys.addAll(MemoryTraversalSideEffects.getMemoryComputeKeys(this.traversal.get()));

        // register MapReducer memory compute keys
        for (final MapReducer<?, ?, ?, ?, ?> mapReducer : TraversalHelper.getStepsOfAssignableClassRecursively(MapReducer.class, this.traversal.get())) {
            this.mapReducers.add(mapReducer.getMapReduce());
            this.memoryComputeKeys.add(MemoryComputeKey.of(mapReducer.getMapReduce().getMemoryKey(), Operator.assign, false, false));
        }
        // register memory computing steps that use memory compute keys
        for (final MemoryComputing<?> memoryComputing : TraversalHelper.getStepsOfAssignableClassRecursively(MemoryComputing.class, this.traversal.get())) {
            this.memoryComputeKeys.add(memoryComputing.getMemoryComputeKey());
            // additional key to launch native spark operation
            if (memoryComputing instanceof SparkOperation) {
                this.memoryComputeKeys.add(SPARK_KEY);
            }
        }
        // register profile steps (TODO: try to hide this)
        for (final ProfileStep profileStep : TraversalHelper.getStepsOfAssignableClassRecursively(ProfileStep.class, this.traversal.get())) {
            this.traversal.get().getSideEffects().register(profileStep.getId(), new MutableMetricsSupplier(profileStep.getPreviousStep()), ProfileStep.ProfileBiOperator.instance());
        }

        this.memoryComputeKeys.add(MemoryComputeKey.of(VOTE_TO_HALT, Operator.and, false, true));
        this.memoryComputeKeys.add(MemoryComputeKey.of(HALTED_TRAVERSERS, Operator.addAll, false, false));
        this.memoryComputeKeys.add(MemoryComputeKey.of(ACTIVE_TRAVERSERS, Operator.addAll, true, true));
        this.memoryComputeKeys.add(MemoryComputeKey.of(MUTATED_MEMORY_KEYS, Operator.addAll, false, true));
        this.memoryComputeKeys.add(MemoryComputeKey.of(COMPLETED_BARRIERS, Operator.addAll, true, true));

        final List<Step> steps = this.traversal.get().getSteps();
        for (final Step step : steps) {
            if (step instanceof RangeGlobalStep) {
                final RangeGlobalStep rangeGlobalStep = (RangeGlobalStep) step;
                if (rangeGlobalStep.getLowRange() != 0) {
                    break;
                }
                final String key = AerospikeComputeKey.createAccumulator(rangeGlobalStep.getId());
                this.memoryComputeKeys.add(MemoryComputeKey.of(key, Operator.sumLong, true, true));
                break;
            }
        }
    }

    @Override
    public Codec getCodec() {
        return codec;
    }

    /**
     * Get the {@link PureTraversal} associated with the current instance of the {@link TraversalVertexProgram}.
     *
     * @return the pure traversal of the instantiated program
     */
    public PureTraversal<?, ?> getTraversal() {
        return this.traversal;
    }

    public static <R> TraverserSet<R> loadHaltedTraversers(final Configuration configuration) {
        if (!configuration.containsKey(HALTED_TRAVERSERS))
            return new TraverserSet<>();

        final Object object = configuration.getProperty(HALTED_TRAVERSERS) instanceof String ?
                VertexProgramHelper.deserialize(configuration, HALTED_TRAVERSERS) :
                configuration.getProperty(HALTED_TRAVERSERS);
        if (object instanceof Traverser.Admin)
            return new TraverserSet<>((Traverser.Admin<R>) object);
        else {
            final TraverserSet<R> traverserSet = new TraverserSet<>();
            traverserSet.addAll((Collection) object);
            return traverserSet;
        }
    }

    public TraversalMatrix<?, ?> getTraversalMatrix() {
        return this.traversalMatrix;
    }

    public static <R> void storeHaltedTraversers(final Configuration configuration, final TraverserSet<R> haltedTraversers) {
        if (null != haltedTraversers && !haltedTraversers.isEmpty()) {
            try {
                VertexProgramHelper.serialize(haltedTraversers, configuration, HALTED_TRAVERSERS);
            } catch (final Exception e) {
                configuration.setProperty(HALTED_TRAVERSERS, haltedTraversers);
            }
        }
    }

    @Override
    public void loadState(final Graph graph, final Configuration configuration) {
        if (!configuration.containsKey(TRAVERSAL))
            throw new IllegalArgumentException("The configuration does not have a traversal: " + TRAVERSAL);
        this.traversal = PureTraversal.loadState(configuration, TRAVERSAL, graph);
        if (!this.traversal.get().isLocked())
            this.traversal.get().applyStrategies();

        this.codec = new TraverserCodec(this.traversal.get());
        /// traversal is compiled and ready to be introspected
        this.traversalMatrix = new TraversalMatrix<>(this.traversal.get());
        // get any master-traversal halted traversers
        this.haltedTraversers = TraversalVertexProgram.loadHaltedTraversers(configuration);
        // if results will be serialized out, don't save halted traversers across the cluster
        this.returnHaltedTraversers =
                (this.traversal.get().getParent().asStep().getNextStep() instanceof ComputerResultStep || // if its just going to stream it out, don't distribute
                        this.traversal.get().getParent().asStep().getNextStep() instanceof EmptyStep ||  // same as above, but if using TraversalVertexProgramStep directly
                        (this.traversal.get().getParent().asStep().getNextStep() instanceof ProfileStep && // same as above, but needed for profiling
                                this.traversal.get().getParent().asStep().getNextStep().getNextStep() instanceof ComputerResultStep));

        init();

        // does the traversal need profile information
        this.profile = !TraversalHelper.getStepsOfAssignableClassRecursively(ProfileStep.class, this.traversal.get()).isEmpty();
        this.db = new DistributedAerospikeConnection((FireflyGraph) graph, jobId);
    }

    @Override
    public void storeState(final Configuration configuration) {
        FireflyProgram.super.storeState(configuration);
        this.traversal.storeState(configuration, TRAVERSAL);
        storeHaltedTraversers(configuration, this.haltedTraversers);
    }

    @Override
    public void setup(final Memory memory) {
        // memory is local
        MemoryTraversalSideEffects.setMemorySideEffects(this.traversal.get(), memory, ProgramPhase.SETUP);
        ((MemoryTraversalSideEffects) this.traversal.get().getSideEffects()).storeSideEffectsInMemory();
        memory.set(VOTE_TO_HALT, true);
        if (memory.exists(SPARK_FLAG)) {
            memory.set(SPARK_FLAG, false);
        }
        memory.set(MUTATED_MEMORY_KEYS, new HashSet<>());
        memory.set(COMPLETED_BARRIERS, new HashSet<>());
        // if halted traversers are being sent from a previous VertexProgram in an OLAP chain (non-distributed traversers), get them into the flow
        if (!this.haltedTraversers.isEmpty()) {
            final TraverserSet<Object> toProcessTraversers = new TraverserSet<>();
            IteratorUtils.removeOnNext(this.haltedTraversers.iterator()).forEachRemaining(traverser -> {
                traverser.setStepId(this.traversal.get().getStartStep().getId());
                toProcessTraversers.add(traverser);
            });
            assert this.haltedTraversers.isEmpty();
            final IndexedTraverserSet<Object, Vertex> remoteActiveTraversers = new IndexedTraverserSet.VertexIndexedTraverserSet();
            BatchMasterExecutor.processTraversers(this.traversal, this.traversalMatrix, toProcessTraversers, remoteActiveTraversers, this.haltedTraversers);
            memory.set(HALTED_TRAVERSERS, this.haltedTraversers);
            memory.set(ACTIVE_TRAVERSERS, remoteActiveTraversers);
        } else {
            memory.set(HALTED_TRAVERSERS, new TraverserSet<>());
            memory.set(ACTIVE_TRAVERSERS, new IndexedTraverserSet.VertexIndexedTraverserSet());
        }
        // local variable will no longer be used so null it for GC
        this.haltedTraversers = null;
        // does the traversal need profile information
        this.profile = !TraversalHelper.getStepsOfAssignableClassRecursively(ProfileStep.class, this.traversal.get()).isEmpty();
    }

    public void execute(final BatchJob job, final Memory memory) {
        // if any global halted traversers, simply don't use them as they were handled by master setup()
        // these halted traversers are typically from a previous OLAP job that yielded traversers at the master traversal
        if (null != this.haltedTraversers)
            this.haltedTraversers = null;
        // memory is distributed
        MemoryTraversalSideEffects.setMemorySideEffects(this.traversal.get(), memory, ProgramPhase.EXECUTE);
        // if a barrier was completed in another worker, it is also completed here (ensure distributed barriers are synchronized)
        final Set<String> completedBarriers = memory.get(COMPLETED_BARRIERS);
        for (final String stepId : completedBarriers) {
            final Step<?, ?> step = this.traversalMatrix.getStepById(stepId);
            if (step instanceof Barrier)
                ((Barrier) this.traversalMatrix.getStepById(stepId)).done();
        }
        // define halted traversers. /dev/null for now.
        final TraverserSet<Object> haltedTraversers = new TraverserSet<>();

        //////////////////
        ((FireflyGraph) traversalMatrix.getTraversal().getGraph().get()).logMessage("Iteration: " + memory.getIteration(), LOGGER);
        if (memory.isInitialIteration()) {    // ITERATION 0
            throw new IllegalStateException("Worker got initial iteration. Please contact support.");
        } else {  // ITERATION 1+
            memory.add(VOTE_TO_HALT,
                    BatchWorkerExecutor.execute(job, this.traversalMatrix, memory, this.returnHaltedTraversers, haltedTraversers));
        }
    }

    @Override
    public boolean terminate(final Memory memory) {
        // memory is local
        MemoryTraversalSideEffects.setMemorySideEffects(this.traversal.get(), memory, ProgramPhase.TERMINATE);
        final boolean voteToHalt = memory.<Boolean>get(VOTE_TO_HALT);
        memory.set(VOTE_TO_HALT, true);
        memory.set(ACTIVE_TRAVERSERS, new IndexedTraverserSet.VertexIndexedTraverserSet());
        if (voteToHalt) {
            // local traverser sets to process
            final TraverserSet<Object> toProcessTraversers = new TraverserSet<>();
            // traversers that need to be sent back to the workers (no longer can be processed locally by the master traversal)
            final IndexedTraverserSet<Object, Vertex> remoteActiveTraversers = new IndexedTraverserSet.VertexIndexedTraverserSet();
            // halted traversers that have completed their journey
            final TraverserSet<Object> haltedTraversers = memory.get(HALTED_TRAVERSERS);
            // get all barrier traversers
            final Set<String> completedBarriers = new HashSet<>();
            BatchMasterExecutor.processMemory(this.traversalMatrix, memory, toProcessTraversers, completedBarriers);
            // process all results from barriers locally and when elements are touched, put them in remoteActiveTraversers
            BatchMasterExecutor.processTraversers(this.traversal, this.traversalMatrix, toProcessTraversers, remoteActiveTraversers, haltedTraversers);
            // tell parallel barriers that might not have been active in the last round that they are no longer active
            memory.set(COMPLETED_BARRIERS, completedBarriers);
            if (!remoteActiveTraversers.isEmpty() ||
                    completedBarriers.stream().map(this.traversalMatrix::getStepById).filter(step -> step instanceof LocalBarrier).findAny().isPresent()) {
                // send active traversers back to workers
                memory.set(ACTIVE_TRAVERSERS, remoteActiveTraversers);
                return false;
            } else {
                // finalize locally with any last traversers dangling in the local traversal
                final Step<?, Object> endStep = (Step<?, Object>) this.traversal.get().getEndStep();
                while (endStep.hasNext()) {
                    haltedTraversers.add(endStep.next());
                }
                // the result of a TraversalVertexProgram are the halted traversers
                memory.set(HALTED_TRAVERSERS, haltedTraversers);
                // finalize profile side-effect steps. (todo: try and hide this)
                for (final ProfileSideEffectStep profileStep : TraversalHelper.getStepsOfAssignableClassRecursively(ProfileSideEffectStep.class, this.traversal.get())) {
                    this.traversal.get().getSideEffects().set(profileStep.getSideEffectKey(), profileStep.generateFinalResult(this.traversal.get().getSideEffects().get(profileStep.getSideEffectKey())));
                }
                return true;
            }
        } else {
            return false;
        }
    }

    @Override
    public void workerIterationStart(final Memory memory) {
        // start collecting profile metrics
        if (this.profile) {
            this.iterationMetrics = new MutableMetrics("iteration" + memory.getIteration(), "Worker Iteration " + memory.getIteration());
            this.iterationMetrics.start();
        }
    }

    @Override
    public void workerIterationEnd(final Memory memory) {
        // store profile metrics in proper ProfileStep metrics
        if (this.profile) {
            final List<ProfileStep> profileSteps = TraversalHelper.getStepsOfAssignableClassRecursively(ProfileStep.class, this.traversal.get());
            // guess the profile step to store data
            int profileStepIndex = memory.getIteration();
            // if we guess wrongly write timing into last step
            profileStepIndex = profileStepIndex >= profileSteps.size() ? profileSteps.size() - 1 : profileStepIndex;
            this.iterationMetrics.finish(0);
            // reset counts
            this.iterationMetrics.setCount(TraversalMetrics.TRAVERSER_COUNT_ID, 0);
            if (null != MemoryTraversalSideEffects.getMemorySideEffectsPhase(this.traversal.get())) {
                this.traversal.get().getSideEffects().add(profileSteps.get(profileStepIndex).getId(), this.iterationMetrics);
            }
            this.iterationMetrics = null;
        }
    }

    @Override
    public void setJobId(final String jobId) {
        this.jobId = jobId;
    }

    @Override
    public void initDB() {
        this.db.deleteTemporaryData();
    }

    @Override
    public void cleanUpDB() {
        this.db.deleteTemporaryData();
    }

    @Override
    public Set<VertexComputeKey> getVertexComputeKeys() {
        return VERTEX_COMPUTE_KEYS;
    }

    @Override
    public Set<MemoryComputeKey> getMemoryComputeKeys() {
        return this.memoryComputeKeys;
    }

    @Override
    public Set<MapReduce> getMapReducers() {
        return this.mapReducers;
    }

    @Override
    public Optional<MessageCombiner<TraverserSet<Object>>> getMessageCombiner() {
        return (Optional) TraversalVertexProgramMessageCombiner.instance();
    }

    @Override
    public TraversalProgram clone() {
        try {
            final TraversalProgram clone = (TraversalProgram) super.clone();
            clone.traversal = this.traversal.clone();
            if (!clone.traversal.get().isLocked())
                clone.traversal.get().applyStrategies();
            clone.traversalMatrix = new TraversalMatrix<>(clone.traversal.get());
            clone.memoryComputeKeys = new HashSet<>();
            for (final MemoryComputeKey memoryComputeKey : this.memoryComputeKeys) {
                clone.memoryComputeKeys.add(memoryComputeKey.clone());
            }
            return clone;
        } catch (final CloneNotSupportedException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
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
        final String traversalString = this.traversal.get().toString().substring(1);
        return StringFactory.vertexProgramString(this, traversalString.substring(0, traversalString.length() - 1));
    }

    @Override
    public Features getFeatures() {
        return new Features() {
            @Override
            public boolean requiresGlobalMessageScopes() {
                return true;
            }

            @Override
            public boolean requiresVertexPropertyAddition() {
                return true;
            }
        };
    }

    //////////////

    public static TraversalProgram.Builder build() {
        return new TraversalProgram.Builder();
    }

    public final static class Builder extends AbstractVertexProgramBuilder<TraversalVertexProgram.Builder> {

        private Builder() {
            super(TraversalProgram.class);
        }

        public TraversalProgram.Builder haltedTraversers(final TraverserSet<Object> haltedTraversers) {
            storeHaltedTraversers(this.configuration, haltedTraversers);
            return this;
        }

        public TraversalProgram.Builder traversal(final TraversalSource traversalSource, final String scriptEngine, final String traversalScript, final Object... bindings) {
            return this.traversal(new ScriptTraversal<>(traversalSource, scriptEngine, traversalScript, bindings));
        }

        public TraversalProgram.Builder traversal(Traversal.Admin<?, ?> traversal) {
            // this is necessary if the job was submitted via TraversalVertexProgram.build() instead of TraversalVertexProgramStep.
            if (!(traversal.getParent() instanceof TraversalVertexProgramStep)) {
                final MemoryTraversalSideEffects memoryTraversalSideEffects = new MemoryTraversalSideEffects(traversal.getSideEffects());
                final Traversal.Admin<?, ?> parentTraversal = new DefaultTraversal<>();
                traversal.getGraph().ifPresent(parentTraversal::setGraph);
                final TraversalStrategies strategies = traversal.getStrategies().clone();
                strategies.addStrategies(ComputerFinalizationStrategy.instance(), ComputerVerificationStrategy.instance(), new VertexProgramStrategy(Computer.compute()));
                parentTraversal.setStrategies(strategies);
                traversal.setStrategies(strategies);
                parentTraversal.setSideEffects(memoryTraversalSideEffects);
                parentTraversal.addStep(new TraversalVertexProgramStep(parentTraversal, traversal));
                traversal = ((TraversalVertexProgramStep) parentTraversal.getStartStep()).getGlobalChildren().get(0);
                traversal.setSideEffects(memoryTraversalSideEffects);
            }
            PureTraversal.storeState(this.configuration, TRAVERSAL, traversal);
            return this;
        }
    }
}
