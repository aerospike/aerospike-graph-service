package com.aerospike.firefly.olap.process.traversal.strategy;

import com.aerospike.firefly.olap.process.traversal.step.SparkSortStep;
import com.aerospike.firefly.process.computer.util.ComputerHelper;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Order;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.RangeGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.OrderGlobalStep;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;
import org.javatuples.Pair;

import java.util.List;

public class SparkOptimizationStrategy extends AbstractTraversalStrategy<TraversalStrategy.ProviderOptimizationStrategy> {

    private static final SparkOptimizationStrategy INSTANCE = new SparkOptimizationStrategy();

    private SparkOptimizationStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
        if (!ComputerHelper.onGraphComputer(traversal))
            return;

        if (!traversal.isRoot() && !(traversal.getParent() instanceof TraversalVertexProgramStep))
            return;

        final List<Step> steps = traversal.getSteps();
        for (int index = 0; index < steps.size(); index++) {
            final Step step = steps.get(index);

            if (!(step instanceof OrderGlobalStep)) {
                continue;
            }

            final OrderGlobalStep orderGlobalStep = (OrderGlobalStep) step;

            // shuffle sorting is not supported so far. Need support for SeedStrategy.
            if (orderGlobalStep.getComparators().stream().anyMatch(c -> ((Pair) c).getValue1() == Order.shuffle)) {
                continue;
            }

            TraversalHelper.replaceStep(step, new SparkSortStep<>(orderGlobalStep), traversal);

            // remove following RangeGlobalStep if no low limit
            // good idea, but not ez to implement with bulking
//            if (index + 1 < steps.size() && steps.get(index + 1) instanceof RangeGlobalStep) {
//                final RangeGlobalStep rangeGlobalStep = (RangeGlobalStep) steps.get(index + 1);
//                if (rangeGlobalStep.getLowRange() == 0 && rangeGlobalStep.getHighRange() == orderGlobalStep.getLimit()) {
//                    traversal.removeStep(rangeGlobalStep);
//                }
//            }
        }
    }

    public static SparkOptimizationStrategy instance() {
        return INSTANCE;
    }
}
