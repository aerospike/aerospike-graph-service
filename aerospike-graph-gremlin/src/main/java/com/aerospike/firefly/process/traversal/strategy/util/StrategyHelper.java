package com.aerospike.firefly.process.traversal.strategy.util;

import com.aerospike.firefly.process.traversal.step.FireflyBatchEdgeReadSampleLimitStep;
import com.aerospike.firefly.process.traversal.step.FireflyBatchEdgeReadStep;
import com.aerospike.firefly.process.traversal.step.FireflyBatchVertexReadSampleLimitStep;
import com.aerospike.firefly.process.traversal.step.FireflyBatchVertexReadStep;
import com.aerospike.firefly.process.traversal.step.FireflyEdgeToVertexBatchReadStep;
import com.aerospike.firefly.process.traversal.step.FireflyOtherVBatchReadStep;
import com.aerospike.firefly.process.traversal.step.computer.FireflyBatchEdgeReadStepLocal;
import com.aerospike.firefly.process.traversal.step.computer.FireflyBatchVertexReadStepLocal;
import com.aerospike.firefly.process.traversal.step.computer.FireflyOtherVBatchReadStepLocal;
import com.aerospike.firefly.process.traversal.step.map.FireflyCountGlobalLocalStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.LambdaHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.RepeatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.PathFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ElementMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ElementStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.FormatStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.MathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PathStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertyMapStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.TreeStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.TreeSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class StrategyHelper {
    private static final Set<Class> STEPS_REQUIRING_PROPERTIES = new HashSet<>(Arrays.asList(
            PathStep.class, PathFilterStep.class, TreeStep.class, TreeSideEffectStep.class, LambdaHolder.class,
            ElementStep.class, MathStep.class, FormatStep.class));

    // always for first step
    public static boolean isPropertyRemovalValid(final Traversal.Admin<?, ?> traversal) {
        final Traversal.Admin<?, ?> root = TraversalHelper.getRootTraversal(traversal);
        return !TraversalHelper.hasStepOfAssignableClassRecursively(STEPS_REQUIRING_PROPERTIES, root);
    }

    public static boolean areEdgesRequired(final Traversal.Admin<?, ?> traversal, final List<Step> steps, final int startIndex) {
        if (traversal.isRoot()) {
            return areEdgesRequired(steps, startIndex < 0 ? 0 : startIndex);
        } else {
            // steps after parent can use in/out edges
            final Step parentStep = traversal.getParent().asStep();
            final Traversal.Admin parentTraversal = parentStep.getTraversal().asAdmin();
            final List<Step> parentSteps = parentTraversal.getSteps();
            return areEdgesRequired(steps, startIndex) || areEdgesRequired(parentTraversal, parentSteps, parentSteps.indexOf(parentStep));
        }
    }

    private static boolean areEdgesRequired(final List<Step> steps, final int startIndex) {
        for (int i = startIndex; i < steps.size(); i++) {
            // raw TinkerPop steps
            if (isVertexOrEdgeStep(steps.get(i))
                    // example: math("b + a").by(in("created").count())
                    || steps.get(i) instanceof MathStep
                    || steps.get(i) instanceof FormatStep
                    // let's play as safe as possible with repeat step
                    || steps.get(i) instanceof RepeatStep.RepeatEndStep) {
                return true;
            }
            // recursively test following steps
            if (steps.get(i) instanceof TraversalParent) {
                final List<Traversal.Admin<Object, Object>> children = new ArrayList<>(((TraversalParent) steps.get(i)).getLocalChildren());
                // for steps like match
                children.addAll(((TraversalParent) steps.get(i)).getGlobalChildren());
                for (final Traversal.Admin child : children) {
                    if (areEdgesRequired(child.getSteps(), 0)) {
                        return true;
                    }
                }
            }
        }

        return false;
    }

    public static boolean isVertexOrEdgeStep(final Step step) {
        return step instanceof VertexStep || step instanceof EdgeVertexStep
                // already replaced FireFly steps
                || step instanceof FireflyCountGlobalLocalStep
                || step instanceof FireflyBatchVertexReadStep
                || step instanceof FireflyBatchVertexReadStepLocal
                || step instanceof FireflyBatchVertexReadSampleLimitStep
                || step instanceof FireflyBatchEdgeReadStep
                || step instanceof FireflyBatchEdgeReadStepLocal
                || step instanceof FireflyBatchEdgeReadSampleLimitStep
                || step instanceof FireflyOtherVBatchReadStep
                || step instanceof FireflyOtherVBatchReadStepLocal
                || step instanceof FireflyEdgeToVertexBatchReadStep;
    }

    public static boolean isPropertyStep(final Step step) {
        return step instanceof PropertiesStep || step instanceof PropertyMapStep
                || step instanceof ElementMapStep;
    }

    public static List<String> getPropertyKeys(final Step step) {
        final String[] propertyKeys;

        if (step instanceof PropertiesStep) {
            propertyKeys = ((PropertiesStep<?>) step).getPropertyKeys();
        } else if (step instanceof PropertyMapStep) {
            propertyKeys = ((PropertyMapStep) step).getPropertyKeys();
        } else {
            propertyKeys = ((ElementMapStep) step).getPropertyKeys();
        }

        if (propertyKeys == null || propertyKeys.length == 0) {
            return Collections.emptyList();
        }

        return Arrays.stream(propertyKeys).filter(k -> k != null && !k.equals(T.id.getAccessor()) && !(k.equals(T.label.getAccessor())))
                .collect(Collectors.toList());
    }

    public static List<String> getPropertyKeys(final List<HasContainer> hasContainers) {
        if (hasContainers == null || hasContainers.isEmpty()) {
            return new ArrayList<>();
        }

        return hasContainers.stream().map(HasContainer::getKey)
                .filter(k -> k != null && !k.equals(T.id.getAccessor()) && !(k.equals(T.label.getAccessor())))
                .collect(Collectors.toList());
    }
}
