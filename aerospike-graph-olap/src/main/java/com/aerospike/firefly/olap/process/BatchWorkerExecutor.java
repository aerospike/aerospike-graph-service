package com.aerospike.firefly.olap.process;

import com.aerospike.firefly.olap.helper.AttachmentHelper;
import com.aerospike.firefly.olap.helper.TaskLogger;
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
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

public class BatchWorkerExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchWorkerExecutor.class);

    private BatchWorkerExecutor() {

    }

    protected static boolean execute(final BatchJob job,
                                     final TraversalMatrix<?, ?> traversalMatrix,
                                     final Memory memory,
                                     final boolean returnHaltedTraversers,
                                     final TraverserSet<Object> haltedTraversers,
                                     final HaltedTraverserStrategy haltedTraverserStrategy) {
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
                    memoryTraversers.add(haltedTraverserStrategy.halt(traverser));
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
        // while there are still local traversers, process them until they leave the vertex (message pass) or halt (store).
        while (!toProcessTraversers.isEmpty()) {
            Step<Object, Object> previousStep = EmptyStep.instance();
            Iterator<Traverser.Admin<Object>> traversers = toProcessTraversers.iterator();
            while (traversers.hasNext()) {
                final Traverser.Admin<Object> traverser = traversers.next();
                traversers.remove();
                final Step<Object, Object> currentStep = traversalMatrix.getStepById(traverser.getStepId());
                // try and fill up the current step as much as possible with traversers to get a bulking optimization
                if (!currentStep.getId().equals(previousStep.getId()) && !(previousStep instanceof EmptyStep))
                    drainStep(previousStep, activeTraversers, haltedTraversers, memory, returnHaltedTraversers, haltedTraverserStrategy);
                currentStep.addStart(traverser);
                previousStep = currentStep;
            }
            drainStep(previousStep, activeTraversers, haltedTraversers, memory, returnHaltedTraversers, haltedTraverserStrategy);
            // all processed traversers should be either halted or active
            assert toProcessTraversers.isEmpty();
            // process all the local objects and send messages or store locally again
            if (!activeTraversers.isEmpty()) {
                traversers = activeTraversers.iterator();
                while (traversers.hasNext()) {
                    final Traverser.Admin<Object> traverser = traversers.next();
                    traversers.remove();
                    // todo: investigate processing elements locally always (!!!)
                    // decide whether to message the traverser or to process it locally
                    if (traverser.get() instanceof Element || traverser.get() instanceof Property) {      // GRAPH OBJECT
                        if (!traverser.isHalted())
                            voteToHalt.set(false);
                        job.addResult(traverser);
                    } else                                                                              // STANDARD OBJECT
                        toProcessTraversers.add(traverser);
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
                                  final boolean returnHaltedTraversers,
                                  final HaltedTraverserStrategy haltedTraverserStrategy) {
        // try execute in slave mode
        GraphComputing.atMaster(step, false);
        TaskLogger.logDebuggingMessage("Drain step: " + step, LOGGER);
        if (step instanceof Barrier && !(step instanceof LocalBarrier)) {
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

            memory.add(TraversalProgram.MUTATED_MEMORY_KEYS, new HashSet<>(Collections.singleton(step.getId())));
        } else { // LOCAL PROCESSING
            final TraverserSet memoryTraversers = new TraverserSet<>();
            step.forEachRemaining(traverser -> {
                if (traverser.isHalted()
                        // if its a ReferenceFactory (one less iteration required)
                        && (returnHaltedTraversers || ReferenceFactory.class == haltedTraverserStrategy.getHaltedTraverserFactory()
                            && !(traverser.get() instanceof Element)
                            && !(traverser.get() instanceof Property))) {
                    if (returnHaltedTraversers) {
                        // todo: double check detachment
                        memoryTraversers.add(haltedTraverserStrategy.halt(traverser));
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
}
