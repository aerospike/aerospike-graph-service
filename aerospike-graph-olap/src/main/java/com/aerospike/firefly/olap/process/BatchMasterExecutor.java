package com.aerospike.firefly.olap.process;

import com.aerospike.firefly.process.computer.util.ComputerHelper;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.traversal.Path;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.ConnectiveStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TailGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.LabelStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyKeyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyValueStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SackStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.HaltedTraverserStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;
import org.apache.tinkerpop.gremlin.structure.util.Attachable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class BatchMasterExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(BatchMasterExecutor.class);

    private BatchMasterExecutor() {

    }

    protected static void processMemory(final TraversalMatrix<?, ?> traversalMatrix, final Memory memory, final TraverserSet<Object> toProcessTraversers, final Set<String> completedBarriers) {
        // handle traversers and data that were sent from the workers to the master traversal via memory
        if (memory.exists(BatchTraversalVertexProgram.MUTATED_MEMORY_KEYS)) {
            for (final String key : memory.<Set<String>>get(BatchTraversalVertexProgram.MUTATED_MEMORY_KEYS)) {
                final Step<Object, Object> step = traversalMatrix.getStepById(key);
                assert step instanceof Barrier;
                completedBarriers.add(step.getId());
                if (!(step instanceof LocalBarrier)) {  // local barriers don't do any processing on the master traversal (they just lock on the workers)
                    final Barrier<Object> barrier = (Barrier<Object>) step;
                    // collecting barriers expect to consume TraverserSet, but spark serialize it as HashSet
                    if (barrier instanceof CollectingBarrierStep) {
                        ComputerHelper.bulkAttach((FireflyGraph) traversalMatrix.getTraversal().getGraph().get(),
                                EmptyTraversalSideEffects.instance(), (TraverserSet) memory.get(key));
                        barrier.addBarrier(memory.get(key));
                    } else {
                        final Object memoryBarrier = memory.get(key);
                        // todo: attach more types if needed
                        if (memoryBarrier instanceof List) {
                            final List list = new ArrayList((List) memoryBarrier);
                            ComputerHelper.bulkAttach((FireflyGraph) traversalMatrix.getTraversal().getGraph().get(),
                                    EmptyTraversalSideEffects.instance(), list);
                            barrier.addBarrier(list);
                        } else if (memoryBarrier instanceof Map) {
                            // for debup barrier it's map obj->traverser
                            final Map map = new HashMap((Map) memoryBarrier);
                            //        at com.aerospike.firefly.process.computer.local.BatchMasterExecutor.processMemory(BatchMasterExecutor.java:74) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at com.aerospike.firefly.process.computer.local.BatchTraversalVertexProgram.terminate(BatchTraversalVertexProgram.java:350) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at com.aerospike.firefly.olap.structure.DistributedGraphComputer.submit(DistributedGraphComputer.java:388) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.VertexProgramStep.processNextStart(VertexProgramStep.java:67) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep.hasNext(AbstractStep.java:155) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.process.traversal.step.util.ExpandableStepIterator.next(ExpandableStepIterator.java:55) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep.processNextStart(ComputerResultStep.java:68) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep.hasNext(AbstractStep.java:155) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal.hasNext(DefaultTraversal.java:192) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.server.util.TraverserIterator.fillBulker(TraverserIterator.java:63) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.server.util.TraverserIterator.hasNext(TraverserIterator.java:50) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.server.op.traversal.TraversalOpProcessor.handleIterator(TraversalOpProcessor.java:350) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.server.op.traversal.TraversalOpProcessor.lambda$iterateBytecodeTraversal$0(TraversalOpProcessor.java:223) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at java.util.concurrent.FutureTask.run(FutureTask.java:264) ~[?:?]
                            //        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515) ~[?:?]
                            //        at java.util.concurrent.FutureTask.run(FutureTask.java:264) ~[?:?]
                            //        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128) ~[?:?]
                            //        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628) ~[?:?]
                            //        at java.lang.Thread.run(Thread.java:829) ~[?:?]
                            //java.util.ConcurrentModificationException
                            //        at java.base/java.util.HashMap$HashIterator.nextNode(HashMap.java:1511)
                            //        at java.base/java.util.HashMap$EntryIterator.next(HashMap.java:1544)
                            //        at java.base/java.util.HashMap$EntryIterator.next(HashMap.java:1542)
                            //        at java.base/java.util.HashMap.putMapEntries(HashMap.java:508)
                            //        at java.base/java.util.HashMap.<init>(HashMap.java:486)
                            //        at com.aerospike.firefly.process.computer.local.BatchMasterExecutor.processMemory(BatchMasterExecutor.java:74)
                            //        at com.aerospike.firefly.process.computer.local.BatchTraversalVertexProgram.terminate(BatchTraversalVertexProgram.java:350)
                            //        at com.aerospike.firefly.olap.structure.DistributedGraphComputer.submit(DistributedGraphComputer.java:388)
                            //        at org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.VertexProgramStep.processNextStart(VertexProgramStep.java:67)
                            //        at org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep.hasNext(AbstractStep.java:155)
                            //        at org.apache.tinkerpop.gremlin.process.traversal.step.util.ExpandableStepIterator.next(ExpandableStepIterator.java:55)
                            //        at org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep.processNextStart(ComputerResultStep.java:68)
                            //        at org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep.hasNext(AbstractStep.java:155)
                            //        at org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal.hasNext(DefaultTraversal.java:192)
                            //        at org.apache.tinkerpop.gremlin.server.util.TraverserIterator.fillBulker(TraverserIterator.java:63)
                            //        at org.apache.tinkerpop.gremlin.server.util.TraverserIterator.hasNext(TraverserIterator.java:50)
                            //        at org.apache.tinkerpop.gremlin.server.op.traversal.TraversalOpProcessor.handleIterator(TraversalOpProcessor.java:350)
                            //        at org.apache.tinkerpop.gremlin.server.op.traversal.TraversalOpProcessor.lambda$iterateBytecodeTraversal$0(TraversalOpProcessor.java:223)
                            //        at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
                            //        at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515)
                            //        at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264)
                            //        at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128)
                            //        at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628)
                            //        at java.base/java.lang.Thread.run(Thread.java:829)
                            //25/02/11 20:25:57 WARN TraversalOpProcessor: Exception processing a Traversal on iteration for request [cd2b17a3-e0bb-488f-8840-fcff75de7867].
                            //java.lang.RuntimeException: Global error 'null' occurred during OLAP traversal.
                            //        at com.aerospike.firefly.olap.structure.DistributedGraphComputer.submit(DistributedGraphComputer.java:437) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.VertexProgramStep.processNextStart(VertexProgramStep.java:67) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep.hasNext(AbstractStep.java:155) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.process.traversal.step.util.ExpandableStepIterator.next(ExpandableStepIterator.java:55) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep.processNextStart(ComputerResultStep.java:68) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep.hasNext(AbstractStep.java:155) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal.hasNext(DefaultTraversal.java:192) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.server.util.TraverserIterator.fillBulker(TraverserIterator.java:63) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.server.util.TraverserIterator.hasNext(TraverserIterator.java:50) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.server.op.traversal.TraversalOpProcessor.handleIterator(TraversalOpProcessor.java:350) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at org.apache.tinkerpop.gremlin.server.op.traversal.TraversalOpProcessor.lambda$iterateBytecodeTraversal$0(TraversalOpProcessor.java:223) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at java.util.concurrent.FutureTask.run(FutureTask.java:264) ~[?:?]
                            //        at java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:515) ~[?:?]
                            //        at java.util.concurrent.FutureTask.run(FutureTask.java:264) ~[?:?]
                            //        at java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1128) ~[?:?]
                            //        at java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:628) ~[?:?]
                            //        at java.lang.Thread.run(Thread.java:829) ~[?:?]
                            //Caused by: java.util.ConcurrentModificationException
                            //        at java.util.HashMap$HashIterator.nextNode(HashMap.java:1511) ~[?:?]
                            //        at java.util.HashMap$EntryIterator.next(HashMap.java:1544) ~[?:?]
                            //        at java.util.HashMap$EntryIterator.next(HashMap.java:1542) ~[?:?]
                            //        at java.util.HashMap.putMapEntries(HashMap.java:508) ~[?:?]
                            //        at java.util.HashMap.<init>(HashMap.java:486) ~[?:?]
                            //        at com.aerospike.firefly.process.computer.local.BatchMasterExecutor.processMemory(BatchMasterExecutor.java:74) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at com.aerospike.firefly.process.computer.local.BatchTraversalVertexProgram.terminate(BatchTraversalVertexProgram.java:350) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        at com.aerospike.firefly.olap.structure.DistributedGraphComputer.submit(DistributedGraphComputer.java:388) ~[aerospike-graph-olap-2.5.0-test14.jar:?]
                            //        ... 16 more
                            ComputerHelper.bulkAttach((FireflyGraph) traversalMatrix.getTraversal().getGraph().get(),
                                    EmptyTraversalSideEffects.instance(), map);
                            barrier.addBarrier(map);
                        } else if (memoryBarrier instanceof Set) {
                            // todo: check incoming TraverserSet?
                            final TraverserSet ts = new TraverserSet();
                            ts.addAll((Set) memoryBarrier);
                            ComputerHelper.bulkAttach((FireflyGraph) traversalMatrix.getTraversal().getGraph().get(),
                                    EmptyTraversalSideEffects.instance(), ts);
                            barrier.addBarrier(ts);
                        } else {
                            barrier.addBarrier(memoryBarrier);
                        }
                    }
                    step.forEachRemaining(toProcessTraversers::add);

                    // some steps can grab reference traversers from memory
                    if (!toProcessTraversers.isEmpty() && step instanceof SideEffectCapStep) {
                        ComputerHelper.bulkAttach((FireflyGraph) traversalMatrix.getTraversal().getGraph().get(),
                                EmptyTraversalSideEffects.instance(), toProcessTraversers);
                    }

                    // if it was a reducing barrier step, reset the barrier to its seed value
                    if (step instanceof ReducingBarrierStep)
                        memory.set(step.getId(), ((ReducingBarrierStep) step).getSeedSupplier().get());
                }
            }
        }
        memory.set(BatchTraversalVertexProgram.MUTATED_MEMORY_KEYS, new HashSet<>());
    }

    protected static void processTraversers(final PureTraversal<?, ?> traversal,
                                            final TraversalMatrix<?, ?> traversalMatrix,
                                            TraverserSet<Object> toProcessTraversers,
                                            // can be used to split work between workers?
                                            final TraverserSet<Object> remoteActiveTraversers,
                                            final TraverserSet<Object> haltedTraversers,
                                            final HaltedTraverserStrategy haltedTraverserStrategy) {

        while (!toProcessTraversers.isEmpty()) {
            final TraverserSet<Object> localActiveTraversers = new TraverserSet<>();
            Step<Object, Object> previousStep = EmptyStep.instance();
            Step<Object, Object> currentStep = EmptyStep.instance();

            // these are traversers that are at the master traversal and will either halt here or be distributed back to the workers as needed
            final Iterator<Traverser.Admin<Object>> traversers = toProcessTraversers.iterator();
            while (traversers.hasNext()) {
                final Traverser.Admin<Object> traverser = traversers.next();
                traversers.remove();
                // traverser.set(DetachedFactory.detach(traverser.get(), true)); // why? following steps will screw up
                traverser.setSideEffects(traversal.get().getSideEffects());
                if (traverser.isHalted())
                    haltedTraversers.add(haltedTraverserStrategy.halt(traverser));
                    // stay local forever (!!!)
//                else if (isRemoteTraverser(traverser, traversalMatrix))  // this is so that patterns like order().name work as expected. try and stay local as long as possible
//                    remoteActiveTraversers.add(traverser.detach());
                else {
                    currentStep = traversalMatrix.getStepById(traverser.getStepId());
                    if (!currentStep.getId().equals(previousStep.getId()) && !(previousStep instanceof EmptyStep)) {
                        GraphComputing.atMaster(previousStep, true);
                        while (previousStep.hasNext()) {
                            final Traverser.Admin<Object> result = previousStep.next();
                            if (result.isHalted())
                                haltedTraversers.add(haltedTraverserStrategy.halt(result));
                                // stay local forever (!!!)
//                            else if (isRemoteTraverser(result, traversalMatrix))
//                                remoteActiveTraversers.add(result.detach());
                            else
                                localActiveTraversers.add(result);
                        }
                    }
                    currentStep.addStart(traverser);
                    previousStep = currentStep;
                }
            }
            if (!(currentStep instanceof EmptyStep)) {
                GraphComputing.atMaster(currentStep, true);
                while (currentStep.hasNext()) {
                    final Traverser.Admin<Object> traverser = currentStep.next();
                    if (traverser.isHalted())
                        haltedTraversers.add(haltedTraverserStrategy.halt(traverser));
                        // stay local forever (!!!)
//                    else if (isRemoteTraverser(traverser, traversalMatrix))
//                        remoteActiveTraversers.add(traverser.detach());
                    else
                        localActiveTraversers.add(traverser);
                }
            }
            assert toProcessTraversers.isEmpty();
            toProcessTraversers = localActiveTraversers;
        }
    }

    private static boolean isRemoteTraverser(final Traverser.Admin traverser, final TraversalMatrix<?, ?> traversalMatrix) {
        return traverser.get() instanceof Attachable &&
                !(traverser.get() instanceof Path) &&
                !isLocalElement(traversalMatrix.getStepById(traverser.getStepId()));
    }

    // TODO: once this is complete (fully known), move to TraversalHelper
    private static boolean isLocalElement(final Step<?, ?> step) {
        return step instanceof PropertiesStep || step instanceof PropertyMapStep ||
                step instanceof IdStep || step instanceof LabelStep || step instanceof SackStep ||
                step instanceof PropertyKeyStep || step instanceof PropertyValueStep ||
                step instanceof TailGlobalStep || step instanceof RangeGlobalStep || step instanceof HasStep ||
                step instanceof ConnectiveStep;
    }
}
