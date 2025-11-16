package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.computer.util.ComputerHelper;
import com.aerospike.firefly.process.traversal.step.FireflyBatchElementMapStep;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.ElementMapStep;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.List;
import java.util.Map;

public class FireflyElementMapStrategy extends FireflyStrategyBase {

    public FireflyElementMapStrategy() {
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        // ElementMapStep on graph computer do not read Vertex labels, so better to keep it as is
        if (ComputerHelper.onGraphComputer(traversal))
            return;

        final FireflyGraph graph = (FireflyGraph) traversal.getGraph().get();

        final List<Step> steps = traversal.getSteps();

        for (int index = 0; index < steps.size(); index++) {
            // If it's not a ElementMapStep, skip it.
            if (!(steps.get(index) instanceof ElementMapStep)) {
                continue;
            }

            final ElementMapStep<Element, Map> elementMapStep = (ElementMapStep) steps.get(index);

            traversal.removeStep(elementMapStep);
            traversal.addStep(index,
                    new FireflyBatchElementMapStep(traversal,
                            elementMapStep.getLabels(),
                            graph.getBaseGraph().getConfig().movementBarrierSize,
                            elementMapStep.getPropertyKeys()));
        }
    }
}
