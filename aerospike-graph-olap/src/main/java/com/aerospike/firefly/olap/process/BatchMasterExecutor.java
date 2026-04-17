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

import com.aerospike.firefly.olap.helper.AttachmentHelper;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.computer.Memory;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.Barrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectCapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ReducingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.EmptyTraversalSideEffects;
import org.apache.tinkerpop.gremlin.process.traversal.util.PureTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalMatrix;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.aerospike.firefly.olap.process.TraversalProgram.MUTATED_MEMORY_KEYS;

public class BatchMasterExecutor {
    private BatchMasterExecutor() {

    }

    protected static void processMemory(final TraversalMatrix<?, ?> traversalMatrix,
                                        final Memory memory,
                                        final TraverserSet<Object> toProcessTraversers,
                                        final Set<String> completedBarriers) {
        // handle traversers and data that were sent from the workers to the master traversal via memory
        if (memory.exists(MUTATED_MEMORY_KEYS)) {
            for (final String key : memory.<Set<String>>get(MUTATED_MEMORY_KEYS)) {
                final Step<Object, Object> step = traversalMatrix.getStepById(key);
                assert step instanceof Barrier;
                completedBarriers.add(step.getId());
                if (!(step instanceof LocalBarrier)) {  // local barriers don't do any processing on the master traversal (they just lock on the workers)
                    final Barrier<Object> barrier = (Barrier<Object>) step;
                    // collecting barriers expect to consume TraverserSet, but spark serialize it as HashSet
                    if (barrier instanceof CollectingBarrierStep) {
                        AttachmentHelper.bulkAttach((FireflyGraph) traversalMatrix.getTraversal().getGraph().get(),
                                EmptyTraversalSideEffects.instance(), (TraverserSet<Object>) memory.get(key));
                        barrier.addBarrier(memory.get(key));
                    } else {
                        final Object memoryBarrier = memory.get(key);
                        // todo: attach more types if needed
                        if (memoryBarrier instanceof List) {
                            final List list = new ArrayList((List) memoryBarrier);
                            AttachmentHelper.bulkAttach((FireflyGraph) traversalMatrix.getTraversal().getGraph().get(), list);
                            barrier.addBarrier(list);
                        } else if (memoryBarrier instanceof Map) {
                            // for debup barrier it's map obj->traverser
                            final Map map = new HashMap((Map) memoryBarrier);
                            AttachmentHelper.bulkAttach((FireflyGraph) traversalMatrix.getTraversal().getGraph().get(),
                                    EmptyTraversalSideEffects.instance(), map);
                            barrier.addBarrier(map);
                        } else if (memoryBarrier instanceof Set) {
                            // todo: check incoming TraverserSet?
                            final TraverserSet<Object> ts = new TraverserSet();
                            ts.addAll((Set) memoryBarrier);
                            AttachmentHelper.bulkAttach((FireflyGraph) traversalMatrix.getTraversal().getGraph().get(),
                                    EmptyTraversalSideEffects.instance(), ts);
                            barrier.addBarrier(ts);
                        } else {
                            barrier.addBarrier(memoryBarrier);
                        }
                    }
                    step.forEachRemaining(toProcessTraversers::add);

                    // some steps can grab reference traversers from memory
                    if (!toProcessTraversers.isEmpty() && step instanceof SideEffectCapStep) {
                        AttachmentHelper.bulkAttach((FireflyGraph) traversalMatrix.getTraversal().getGraph().get(),
                                EmptyTraversalSideEffects.instance(), toProcessTraversers);
                    }

                    // if it was a reducing barrier step, reset the barrier to its seed value
                    if (step instanceof ReducingBarrierStep)
                        memory.set(step.getId(), ((ReducingBarrierStep) step).getSeedSupplier().get());
                }
            }
        }
        memory.set(MUTATED_MEMORY_KEYS, new HashSet<>());
    }

    protected static void processTraversers(final PureTraversal<?, ?> traversal,
                                            final TraversalMatrix<?, ?> traversalMatrix,
                                            TraverserSet<Object> toProcessTraversers,
                                            final TraverserSet<Object> remoteActiveTraversers,
                                            final TraverserSet<Object> haltedTraversers) {

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
                    haltedTraversers.add(traverser);
                else {
                    currentStep = traversalMatrix.getStepById(traverser.getStepId());
                    if (!currentStep.getId().equals(previousStep.getId()) && !(previousStep instanceof EmptyStep)) {
                        GraphComputing.atMaster(previousStep, true);
                        while (previousStep.hasNext()) {
                            final Traverser.Admin<Object> result = previousStep.next();
                            if (result.isHalted())
                                haltedTraversers.add(result);
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
                        haltedTraversers.add(traverser);
                    else
                        localActiveTraversers.add(traverser);
                }
            }
            assert toProcessTraversers.isEmpty();
            toProcessTraversers = localActiveTraversers;
        }
    }
}
