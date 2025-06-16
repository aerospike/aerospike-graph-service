package com.aerospike.firefly.olap.process;

import com.aerospike.firefly.olap.config.QueryParameters;
import com.aerospike.firefly.olap.helper.AttachmentHelper;
import com.aerospike.firefly.olap.helper.TaskLogger;
import com.aerospike.firefly.olap.process.traversal.step.SparkOperation;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.traversal.TraversalVertexProgram;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.Bypassing;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.Parameterizing;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.Parameters;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.aerospike.firefly.olap.process.TraversalProgram.MUTATED_MEMORY_KEYS;

public class BatchWorkerExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchWorkerExecutor.class);

    private BatchWorkerExecutor() {

    }

    protected static boolean execute(final BatchJob job,
                                     final TraversalMatrix<?, ?> traversalMatrix,
                                     final Memory memory,
                                     final boolean returnHaltedTraversers,
                                     final TraverserSet<Object> haltedTraversers) {
        final TraversalSideEffects traversalSideEffects = traversalMatrix.getTraversal().getSideEffects();
        final AtomicBoolean voteToHalt = new AtomicBoolean(true);
        final TraverserSet<Object> activeTraversers = new TraverserSet<>();
        final TraverserSet<Object> toProcessTraversers = new TraverserSet<>();

        ////////////////////////////////
        // GENERATE LOCAL TRAVERSERS //
        ///////////////////////////////

        final TraverserSet memoryTraversers = new TraverserSet<>();
        IteratorUtils.removeOnNext(job.getStarts().iterator()).forEachRemaining(traverser -> {
            if (traverser.isHalted()) {
                if (returnHaltedTraversers)
                    memoryTraversers.add(traverser); // always reference
                else
                    haltedTraversers.add(traverser); // the traverser has already been detached so no need to detach it again
            } else {
                toProcessTraversers.add(traverser);
            }
        });
        if (!memoryTraversers.isEmpty()) {
            memory.add(TraversalVertexProgram.HALTED_TRAVERSERS, memoryTraversers);
        }

        AttachmentHelper.bulkAttach((FireflyGraph) traversalMatrix.getTraversal().getGraph().get(),
                traversalSideEffects, toProcessTraversers);

        ///////////////////////////////
        // PROCESS LOCAL TRAVERSERS //
        //////////////////////////////
        // while there are still local traversers, process them until we have not result size growth. Or halt (store).
        while (!toProcessTraversers.isEmpty()) {
            boolean canContinueOnSameWorker = true;
            Step<Object, Object> previousStep = EmptyStep.instance();

            // group traversers by step
            toProcessTraversers.sort(Comparator.comparing(traverser -> traverser.asAdmin().getStepId()));
            Iterator<Traverser.Admin<Object>> traversers = toProcessTraversers.iterator();
            while (traversers.hasNext()) {
                final Traverser.Admin<Object> traverser = traversers.next();
                traversers.remove();
                final Step<Object, Object> currentStep = traversalMatrix.getStepById(traverser.getStepId());
                canContinueOnSameWorker = canContinue(currentStep, canContinueOnSameWorker);
                // try and fill up the current step as much as possible with traversers to get a bulking optimization
                if (!currentStep.getId().equals(previousStep.getId()) && !(previousStep instanceof EmptyStep))
                    drainStep(previousStep, activeTraversers, haltedTraversers, memory, returnHaltedTraversers);
                currentStep.addStart(traverser);
                previousStep = currentStep;
            }
            drainStep(previousStep, activeTraversers, haltedTraversers, memory, returnHaltedTraversers);
            // all processed traversers should be either halted or active
            assert toProcessTraversers.isEmpty();
            // process all the local objects and send messages or store locally again
            if (!activeTraversers.isEmpty()) {
                traversers = activeTraversers.iterator();
                while (traversers.hasNext()) {
                    final Traverser.Admin<Object> traverser = traversers.next();
                    traversers.remove();

                    if (!canContinueOnSameWorker) {
                        if (!traverser.isHalted())
                            voteToHalt.set(false);
                        job.addResult(traverser);
                    } else {
                        toProcessTraversers.add(traverser);
                    }
                }
                assert activeTraversers.isEmpty();
            }
        }
        return voteToHalt.get();
    }

    private static void drainStep(final Step<Object, Object> step,
                                  final TraverserSet<Object> activeTraversers,
                                  final TraverserSet<Object> haltedTraversers,
                                  final Memory memory,
                                  final boolean returnHaltedTraversers) {
        // try execute in slave mode
        GraphComputing.atMaster(step, false);
        TaskLogger.logDebuggingMessage("Drain step: " + step, LOGGER);
        if (step instanceof SparkOperation) {
            // replace traverser's with ProjectedTraverser's
            final Barrier barrier = (Barrier) step;
            while (barrier.hasNextBarrier()) {
                activeTraversers.addAll((TraverserSet) barrier.nextBarrier());
            }
            memory.add(TraversalProgram.SPARK_FLAG, true);
        } else if (step instanceof Barrier && !(step instanceof LocalBarrier)) {
            if (step instanceof Bypassing)
                ((Bypassing) step).setBypass(true);

            final Barrier barrier = (Barrier) step;
            if (barrier.hasNextBarrier()) {
                while (barrier.hasNextBarrier()) {
                    memory.add(step.getId(), barrier.nextBarrier());
                }
            } else {
                // ensure the step id gets added to memory or else barriers that filter like order().by('no-exist')
                // will end in error when that memory key can't be found by MasterExecutor.processMemory()
                memory.add(step.getId(), new TraverserSet<>());
            }

            memory.add(MUTATED_MEMORY_KEYS, new HashSet<>(Collections.singleton(step.getId())));
        } else { // LOCAL PROCESSING
            final TraverserSet memoryTraversers = new TraverserSet<>();
            step.forEachRemaining(traverser -> {
                if (traverser.isHalted()) {
                    if (returnHaltedTraversers) {
                        memoryTraversers.add(traverser.detach());
                    } else {
                        haltedTraversers.add(traverser.detach());
                    }
                } else {
                    activeTraversers.add(traverser);
                }
            });
            if (!memoryTraversers.isEmpty())
                memory.add(TraversalVertexProgram.HALTED_TRAVERSERS, memoryTraversers);
        }
        final String info = String.format("Drain step complete: %s [%d %d]", step, activeTraversers.size(), haltedTraversers.size());
        TaskLogger.logDebuggingMessage(info, LOGGER);
    }

    private static boolean canContinue(final Step step, final boolean currentState) {
        if (!currentState) {
            return false;
        }

        // need to wait for results of all barriers, including LocalBarrier like AggregateGlobalStep
        if (step instanceof Barrier) {
            return false;
        }

        // override in concrete step
        if (step instanceof Parameterizing) {
            final Parameters parameters = ((Parameterizing) step).getParameters();
            if (parameters.contains(QueryParameters.REPARTITION)) {
                final List asList = parameters.get(QueryParameters.REPARTITION, null);
                if (!asList.isEmpty())
                    return (Boolean) asList.get(asList.size() - 1);
            }
        }

        // parent is VertexProgramStep
        if (((Step)step.getTraversal().getParent()).getTraversal().isRoot() && step instanceof VertexStep
                && !(step.getNextStep() instanceof Barrier) && !(step.getNextStep() instanceof EmptyStep)) {
            return false;
        }

        return true;
    }
}
