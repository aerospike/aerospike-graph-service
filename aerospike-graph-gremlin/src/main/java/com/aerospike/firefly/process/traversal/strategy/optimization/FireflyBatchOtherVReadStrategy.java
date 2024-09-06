package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.step.FireflyBatchEdgeReadStep;
import com.aerospike.firefly.process.traversal.step.FireflyBatchEdgeSampleLimitReadStep;
import com.aerospike.firefly.process.traversal.step.FireflyOtherVBatchReadStep;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.util.ConfigurationHelper;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.SampleGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.EdgeOtherVertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GroupStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.GroupSideEffectStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Set;

public class FireflyBatchOtherVReadStrategy extends FireflyStrategyBase {

    final ThreadLocal<Boolean> rootGroup = new ThreadLocal<Boolean>() {
        @Override
        protected Boolean initialValue() {
            return false;
        }
    };

    public FireflyBatchOtherVReadStrategy() {
    }

    @Override
    public String getStrategyEnabledKey() {
        return ConfigurationHelper.Keys.ENABLE_BATCH_VERTEX_READ_OTHERV_STRATEGY;
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();

        // Reset whenever root.
        if (traversal.isRoot()) {
            rootGroup.set(false);
        }

        if (!traversal.isRoot()) {
            if (rootGroup.get()) {
                return;
            }
            // todo: !!!
            if (!graph.getBaseGraph().ENABLE_BATCH_VERTEX_READ_OTHERV_STRATEGY) {
                return;
            }
        }

        if (TraversalHelper.onGraphComputer(traversal))
            return;
        final List<Step> steps = traversal.getSteps();

        if (traversal.isRoot()) {
            for (int i = 0; i < steps.size(); i++) {
                if (steps.get(i) instanceof GroupStep || steps.get(i) instanceof GroupSideEffectStep) {
                    rootGroup.set(true);
                    break;
                }
            }
        }

        for (int index = 0; index < steps.size(); index++) {
            if (!(steps.get(index) instanceof EdgeOtherVertexStep)) {
                continue;
            }

            traversal.removeStep(steps.get(index));

            traversal.addStep(index, new FireflyOtherVBatchReadStep(
                    traversal,
                    graph.getBaseGraph().MOVEMENT_BARRIER_SIZE));
        }
    }
}
