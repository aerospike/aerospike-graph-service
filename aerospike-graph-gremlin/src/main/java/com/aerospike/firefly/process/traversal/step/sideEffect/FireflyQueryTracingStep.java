package com.aerospike.firefly.process.traversal.step.sideEffect;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Operator;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.GraphComputing;
import org.apache.tinkerpop.gremlin.process.traversal.step.SideEffectCapable;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.SideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.util.DefaultTraversalMetrics;
import org.apache.tinkerpop.gremlin.util.function.DefaultTraversalMetricsSupplier;

import java.util.function.Supplier;

import static com.aerospike.firefly.process.traversal.step.util.TraversalUtil.toStringScript;
import static org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.ProfileSideEffectStep.DEFAULT_METRICS_KEY;

public class FireflyQueryTracingStep<S> extends SideEffectStep<S>
        implements SideEffectCapable<DefaultTraversalMetrics, DefaultTraversalMetrics>, GraphComputing {

    private String sideEffectKey;
    private boolean onGraphComputer = false;

    public FireflyQueryTracingStep(final Traversal.Admin traversal) {
        super(traversal);
        this.sideEffectKey = DEFAULT_METRICS_KEY;
        this.getTraversal().getSideEffects().registerIfAbsent(this.sideEffectKey,
                (Supplier) DefaultTraversalMetricsSupplier.instance(), Operator.assign);
    }

    @Override
    protected void sideEffect(final Traverser.Admin<S> traverser) {
    }

    @Override
    public String getSideEffectKey() {
        return this.sideEffectKey;
    }

    @Override
    public Traverser.Admin<S> next() {
        Traverser.Admin<S> start = null;
        try {
            start = super.next();
            return start;
        } finally {
            if (!this.onGraphComputer && start == null) {
                processProfileMetrics();
            }
        }
    }

    @Override
    public boolean hasNext() {
        boolean start = super.hasNext();
        if (!this.onGraphComputer && !start) {
            processProfileMetrics();
        }
        return start;
    }

    private DefaultTraversalMetrics getTraversalMetricsFromSideEffects() {
        return this.getTraversal().getSideEffects().get(this.sideEffectKey);
    }

    private void processProfileMetrics() {
        final DefaultTraversalMetrics m = getTraversalMetricsFromSideEffects();
        if (!m.isFinalized()) {
            m.setMetrics(this.getTraversal(), false);
            final FireflyGraph graph = (FireflyGraph) this.traversal.getGraph().get();
            graph.exportQuery(getTraversalMetricsFromSideEffects(),
                    "FireflyProfileSideEffectStep",
                    toStringScript(traversal, graph.getBaseGraph().getConfig().redactScriptLiteralsEnabled));
        }
    }

    @Override
    public DefaultTraversalMetrics generateFinalResult(final DefaultTraversalMetrics tm) {
        if (this.onGraphComputer && !tm.isFinalized())
            tm.setMetrics(this.getTraversal(), true);
        return tm;
    }

    @Override
    public void onGraphComputer() {
        onGraphComputer = true;
    }
}
