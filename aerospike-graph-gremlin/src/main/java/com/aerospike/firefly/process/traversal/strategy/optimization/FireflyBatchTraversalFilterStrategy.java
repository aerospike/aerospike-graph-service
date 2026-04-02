package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.computer.util.ComputerHelper;
import com.aerospike.firefly.process.traversal.step.FireflyBatchVertexReadStep;
import com.aerospike.firefly.util.config.ConfigurationHelper;
import com.aerospike.firefly.util.config.ConfigurationHelper.TraversalOptions;
import org.apache.tinkerpop.gremlin.process.traversal.Pop;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.DedupGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.TraversalFilterStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.SelectOneStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.EmptyStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Rewrites {@code TraversalFilterStep} whose child traversal contains at least one
 * {@link LocalBarrier} step (e.g. {@link FireflyBatchVertexReadStep},
 * {@code FireflyBatchEdgeReadStep}, {@code FireflyEdgeToVertexBatchReadStep}, etc.) from:
 * <pre>
 *   [prev, TraversalFilterStep([childStep1, childStep2, ...])]
 * </pre>
 * into the equivalent batched form:
 * <pre>
 *   [prev@[label], childStep1, childStep2, ..., SelectOneStep(label), DedupGlobalStep]
 * </pre>
 * By inlining the child traversal into the parent, each barrier step operates at the top
 * level where it can collect traversers in bulk rather than being fed one at a time by
 * {@code TraversalFilterStep}.
 * <p>
 * Must run <b>after</b> {@link FireflyBatchVertexReadStrategy} and
 * {@link FireflyBatchEdgeReadStrategy}.
 * <p>
 * <b>Experimental</b> — must be explicitly enabled per-traversal via
 * {@code g.with("aerospike.graph.filter.optimization.enabled", true).V().where(...)}.
 */
public class FireflyBatchTraversalFilterStrategy extends FireflyStrategyBase {

    private static final Logger LOG = LoggerFactory.getLogger(FireflyBatchTraversalFilterStrategy.class);
    private static final AtomicInteger LABEL_COUNTER = new AtomicInteger(0);
    private static final Field REQUIREMENTS_FIELD;

    static {
        Field f = null;
        try {
            f = DefaultTraversal.class.getDeclaredField("requirements");
            f.setAccessible(true);
        } catch (final NoSuchFieldException e) {
            LOG.warn("Cannot access DefaultTraversal.requirements; as/select/dedup filter rewrite disabled", e);
        }
        REQUIREMENTS_FIELD = f;
    }

    @Override
    public String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_COMPOSITE_ID_STRATEGY;
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        if (REQUIREMENTS_FIELD == null || ComputerHelper.onGraphComputer(traversal))
            return;

        final Optional<Boolean> enabled = ConfigurationHelper.getTraversalOptionBoolean(
                TraversalOptions.FILTER_OPTIMIZATION_ENABLED, traversal, false);
        if (enabled.isEmpty() || !enabled.get())
            return;

        final List<TraversalFilterStep> filterSteps =
                TraversalHelper.getStepsOfClass(TraversalFilterStep.class, traversal);

        boolean rewritten = false;

        for (final TraversalFilterStep<?> filterStep : filterSteps) {
            final List<Step> childSteps = filterStep.getFilterTraversal().getSteps();

            boolean hasBatchStep = false;
            for (final Step step : childSteps) {
                if (step instanceof LocalBarrier) {
                    hasBatchStep = true;
                    break;
                }
            }
            if (!hasBatchStep)
                continue;

            final Step<?, ?> prevStep = filterStep.getPreviousStep();
            if (prevStep instanceof EmptyStep)
                continue;

            final String filterLabel = "__ff" + LABEL_COUNTER.getAndIncrement();
            prevStep.addLabel(filterLabel);

            int insertIndex = TraversalHelper.stepIndex(filterStep, traversal);
            traversal.removeStep(filterStep);

            for (final Step childStep : childSteps) {
                traversal.addStep(insertIndex++, childStep);
            }

            final SelectOneStep selectStep = new SelectOneStep(traversal, Pop.last, filterLabel);
            traversal.addStep(insertIndex++, selectStep);

            final DedupGlobalStep dedupStep = new DedupGlobalStep(traversal);
            for (final String label : filterStep.getLabels()) {
                dedupStep.addLabel(label);
            }
            traversal.addStep(insertIndex, dedupStep);
            rewritten = true;
        }

        if (rewritten) {
            invalidateCachedRequirements(traversal);
        }
    }

    /**
     * TinkerPop's {@code LazyBarrierStrategy} caches traverser requirements before provider
     * strategies run. After we add {@link SelectOneStep} (which needs {@code LABELED_PATH}),
     * the cached requirements are stale. Nulling the field forces recomputation at iteration time.
     */
    private static void invalidateCachedRequirements(final Traversal.Admin<?, ?> traversal) {
        final Traversal.Admin<?, ?> root = TraversalHelper.getRootTraversal(traversal);
        try {
            REQUIREMENTS_FIELD.set(root, null);
        } catch (final IllegalAccessException e) {
            LOG.warn("Failed to reset traverser requirements", e);
        }
    }

    private static final FireflyBatchTraversalFilterStrategy INSTANCE = new FireflyBatchTraversalFilterStrategy();

    public static FireflyBatchTraversalFilterStrategy instance() {
        return INSTANCE;
    }
}
