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

package com.aerospike.firefly.process.traversal.strategy.verification;

import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ShortestPathVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.VertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.Mutating;
import org.apache.tinkerpop.gremlin.process.traversal.step.PathProcessor;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ElementStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.InjectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.IoStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SubgraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.ProfileStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.Set;
import java.util.function.BinaryOperator;

public final class FireflyComputerVerificationStrategy extends AbstractTraversalStrategy<TraversalStrategy.VerificationStrategy> implements TraversalStrategy.VerificationStrategy {
    private static final FireflyComputerVerificationStrategy INSTANCE = new FireflyComputerVerificationStrategy();
    private static final Set<Class<?>> UNSUPPORTED_ALGORITHM_STEPS = Set.of(ShortestPathVertexProgramStep.class);
    private static final Set<Class<?>> UNSUPPORTED_STEPS = Set.of(
            InjectStep.class, Mutating.class, SubgraphStep.class, ComputerResultStep.class, IoStep.class, ElementStep.class
    );
    private static final Set<Operator> UNSUPPORTED_OPERATORS = Set.of(Operator.minus, Operator.div);

    private FireflyComputerVerificationStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        // copy-pasted from ComputerVerificationStrategy with some changes
        if (traversal.getParent() instanceof TraversalVertexProgramStep) {
            if (TraversalHelper.getStepsOfAssignableClassRecursively(GraphStep.class, traversal).size() > 1)
                throw new VerificationException("Mid-traversal V()/E() is currently not supported on GraphComputer", traversal);
            if (TraversalHelper.hasStepOfAssignableClassRecursively(ProfileStep.class, traversal) && TraversalHelper.getStepsOfAssignableClass(VertexProgramStep.class, TraversalHelper.getRootTraversal(traversal)).size() > 1)
                throw new VerificationException("Profiling a multi-VertexProgramStep traversal is currently not supported on GraphComputer", traversal);
        }

        // this is a problem because sideEffect.merge() is transient on the OLAP reduction
        if (TraversalHelper.getRootTraversal(traversal).getTraverserRequirements().contains(TraverserRequirement.ONE_BULK))
            throw new VerificationException("One bulk is currently not supported on GraphComputer: " + traversal, traversal);

        // sack is a problem because the sack value is not serialized across OLAP workers
        if (TraversalHelper.getRootTraversal(traversal).getTraverserRequirements().contains(TraverserRequirement.SACK))
            throw new VerificationException("Sack is currently not supported on GraphComputer: " + traversal, traversal);

//        // you can not traverse past the local star graph with localChildren (e.g. by()-modulators).
//        if (!TraversalHelper.isGlobalChild(traversal) && !TraversalHelper.isLocalStarGraph(traversal))
//            throw new VerificationException("Local traversals may not traverse past the local star-graph on GraphComputer: " + traversal, traversal);

        for (final Step<?, ?> step : traversal.getSteps()) {
            // there is issue with nested select step, let's keep this restriction for now
            if (step instanceof PathProcessor && ((PathProcessor) step).getMaxRequirement() != PathProcessor.ElementRequirement.ID)
                throw new VerificationException("It is not possible to access more than a path element's id on GraphComputer: " + step + " requires " + ((PathProcessor) step).getMaxRequirement(), traversal);

            if (UNSUPPORTED_STEPS.stream().anyMatch(c -> c.isAssignableFrom(step.getClass()))
                    // ComputerResultStep is only allowed as the final step of the root traversal
                    && (!traversal.isRoot() || !(step instanceof ComputerResultStep)))
                throw new VerificationException("The following step is currently not supported on GraphComputer: " + step, traversal);

            if (step instanceof SideEffectCapable) {
                final BinaryOperator<?> sideEffectOperator = traversal.getSideEffects().getReducer(((SideEffectCapable<?, ?>) step).getSideEffectKey());
                if (UNSUPPORTED_OPERATORS.stream().anyMatch(o -> o == sideEffectOperator)) {
                    throw new VerificationException("The following step has an SideEffect operator " + sideEffectOperator + " which is currently not supported on GraphComputer: " + step, traversal);
                }
            }
        }

        // // there is issue with nested select step, let's keep this restriction for now
        Step<?, ?> nextParentStep = traversal.getParent().asStep();
        while (!(nextParentStep instanceof EmptyStep)) {
            if (nextParentStep instanceof PathProcessor && ((PathProcessor) nextParentStep).getMaxRequirement() != PathProcessor.ElementRequirement.ID)
                throw new VerificationException("The following path processor step requires more than the element id on GraphComputer: " + nextParentStep + " requires " + ((PathProcessor) nextParentStep).getMaxRequirement(), traversal);
            nextParentStep = nextParentStep.getNextStep();
        }
        // end of ComputerVerificationStrategy

        // ADDING A PRE-CHECK
        // TraversalVertexProgram is decomposed into various individual VertexPrograms when using the graph algorithms package
        traversal.getSteps().forEach(step -> {
            if (UNSUPPORTED_ALGORITHM_STEPS.stream().filter(c -> c.isAssignableFrom(step.getClass())).findFirst().isPresent())
                throw new VerificationException("The following step is currently not supported on GraphComputer: " + step, traversal);
        });
    }

    public static FireflyComputerVerificationStrategy instance() {
        return INSTANCE;
    }
}
