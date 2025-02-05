package com.aerospike.firefly.process.computer.local;

import com.aerospike.firefly.process.computer.util.ComputerHelper;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.computer.MessageScope;
import org.apache.tinkerpop.gremlin.process.computer.Messenger;
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
import org.apache.tinkerpop.gremlin.process.traversal.traverser.ProjectedTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.IndexedTraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.Host;
import org.apache.tinkerpop.gremlin.structure.util.reference.ReferenceFactory;
import org.apache.tinkerpop.gremlin.util.iterator.IteratorUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;

public class BatchWorkerExecutor {

    private BatchWorkerExecutor() {

    }

    protected static boolean execute(final Messenger<TraverserSet<Object>> messenger,
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

        // MASTER ACTIVE
        // these are traversers that are going from OLTP (master) to OLAP (workers)
        // these traversers were broadcasted from the master traversal to the workers for attachment
        final IndexedTraverserSet<Object, Vertex> maybeActiveTraversers = memory.get(TraversalVertexProgram.ACTIVE_TRAVERSERS);
        System.out.println(Thread.currentThread().getId() + "   memory maybeActiveTraversers: " + toProcessTraversers);
        // some memory systems are interacted with by multiple threads and thus, concurrent modification can happen at iterator.remove().
        // its better to reduce the memory footprint and shorten the active traverser list so synchronization is worth it.
        // most distributed OLAP systems have the memory partitioned and thus, this synchronization does nothing.
        // todo: read necessary vertices for attachment
//        final List<? extends Vertex> vertexCache = vertices;
//        synchronized (maybeActiveTraversers) {
//            if (!maybeActiveTraversers.isEmpty()) {
//                for (final Vertex vertex : vertexCache) {
//                    final Collection<Traverser.Admin<Object>> traversers = maybeActiveTraversers.get(vertex);
//                    if (traversers != null) {
//                        final Iterator<Traverser.Admin<Object>> iterator = traversers.iterator();
//                        while (iterator.hasNext()) {
//                            final Traverser.Admin<Object> traverser = iterator.next();
//                            iterator.remove();
//                            maybeActiveTraversers.remove(traverser);
//                            traverser.attach(Attachable.Method.get(vertex));
//                            traverser.setSideEffects(traversalSideEffects);
//                            toProcessTraversers.add(traverser);
//                        }
//                    }
//                }
//            }
//        }

        // WORKER ACTIVE
        // these are traversers that exist from a local barrier
        // these traversers will simply saved at the local vertex while the master traversal synchronized the barrier
        // todo: local barrier magic
//        vertex.<TraverserSet<Object>>property(TraversalVertexProgram.ACTIVE_TRAVERSERS).ifPresent(previousActiveTraversers -> {
//            IteratorUtils.removeOnNext(previousActiveTraversers.iterator()).forEachRemaining(traverser -> {
//                traverser.attach(Attachable.Method.get(vertex));
//                traverser.setSideEffects(traversalSideEffects);
//                toProcessTraversers.add(traverser);
//            });
//            assert previousActiveTraversers.isEmpty();
//            // remove the property to save space
//            vertex.property(TraversalVertexProgram.ACTIVE_TRAVERSERS).remove();
//        });

        // TRAVERSER MESSAGES (WORKER -> WORKER)
        // these are traversers that have been messaged to the vertex from another vertex
        final Iterator<TraverserSet<Object>> messages = messenger.receiveMessages();
        while (messages.hasNext()) {
            IteratorUtils.removeOnNext(messages.next().iterator()).forEachRemaining(traverser -> {
                System.out.println(Thread.currentThread().getId() + "   message: " + traverser + "; bulk: " + traverser.bulk());
                if (traverser.isHalted()) {
                    if (returnHaltedTraversers)
                        memory.add(TraversalVertexProgram.HALTED_TRAVERSERS, new TraverserSet<>(haltedTraverserStrategy.halt(traverser)));
                    else
                        haltedTraversers.add(traverser); // the traverser has already been detached so no need to detach it again
                } else {
                    // traverser is not halted and thus, should be processed locally
                    // attach it and process
//                    Vertex vertex = null;
//                    for (final Vertex v : vertexCache) {
//                        if (ElementHelper.areEqual(v, traverser.get())) {
//                            vertex = v;
//                            break;
//                        }
//                    }
//                    // todo: null check (?)
//                    traverser.attach(Attachable.Method.get(vertex));
//                    traverser.setSideEffects(traversalSideEffects);
                    toProcessTraversers.add(traverser);
                }
            });
        }

        ComputerHelper.bulkAttach((FireflyGraph) traversalMatrix.getTraversal().getGraph().get(),
                traversalSideEffects, toProcessTraversers);

        ///////////////////////////////
        // PROCESS LOCAL TRAVERSERS //
        //////////////////////////////
        System.out.println(Thread.currentThread().getId() + "   message toProcessTraversers: " + toProcessTraversers);
        // while there are still local traversers, process them until they leave the vertex (message pass) or halt (store).
        while (!toProcessTraversers.isEmpty()) {
            Step<Object, Object> previousStep = EmptyStep.instance();
            Iterator<Traverser.Admin<Object>> traversers = toProcessTraversers.iterator();
            while (traversers.hasNext()) {
                final Traverser.Admin<Object> traverser = traversers.next();
                traversers.remove();
                final Step<Object, Object> currentStep = traversalMatrix.getStepById(traverser.getStepId());
                System.out.println(Thread.currentThread().getId() + " WorkerExecutor.execute: " + traverser + "; " + currentStep);
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
                    // todo: cleanup
                    // decide whether to message the traverser or to process it locally
                    if (traverser.get() instanceof Element || traverser.get() instanceof Property) {      // GRAPH OBJECT
                        // if the element is remote, then message, else store it locally for re-processing
                        final Vertex hostingVertex = Host.getHostingVertex(traverser.get());
                        if (!traverser.isHalted())
                            voteToHalt.set(false);
                        messenger.sendMessage(MessageScope.Global.of(hostingVertex), new TraverserSet<>(traverser.detach()));
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
        System.out.println(Thread.currentThread().getId() + " WorkerExecutor.drainStep step " + step +
                "; activeTraversers" + activeTraversers +
                "; haltedTraversers" + haltedTraversers);
        // try execute in slave mode
        GraphComputing.atMaster(step, false);
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

            memory.add(BatchTraversalVertexProgram.MUTATED_MEMORY_KEYS, new HashSet<>(Collections.singleton(step.getId())));
        } else { // LOCAL PROCESSING
            step.forEachRemaining(traverser -> {
                System.out.println("  working on " + traverser + "; halted: " + traverser.isHalted());
                if (traverser.isHalted() &&
                        // if its a ReferenceFactory (one less iteration required)
                        ((returnHaltedTraversers || ReferenceFactory.class == haltedTraverserStrategy.getHaltedTraverserFactory()) &&
                                (!(traverser.get() instanceof Element) && !(traverser.get() instanceof Property)) /*||
                                vertices.contains(Host.getHostingVertex(traverser.get()))*/)) {
                    if (returnHaltedTraversers) {
                        System.out.println("    memory.add");
                        ComputerHelper.prepareForDistributedMemory(traverser);
                        memory.add(TraversalVertexProgram.HALTED_TRAVERSERS, new TraverserSet<>(haltedTraverserStrategy.halt(traverser)));
                    } else {
                        System.out.println("    haltedTraversers.add");
                        haltedTraversers.add(traverser.detach());
                    }
                } else {
                    System.out.println("    activeTraversers.add");
                    activeTraversers.add(traverser);
                }
            });
        }

        System.out.println(Thread.currentThread().getId() + " WorkerExecutor.drainStep done. activeTraversers" + activeTraversers +
                "; haltedTraversers" + haltedTraversers);
    }

}
