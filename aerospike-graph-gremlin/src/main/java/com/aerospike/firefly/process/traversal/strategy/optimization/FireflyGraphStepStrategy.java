package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.computer.local.ComputerHelper;
import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.step.HasContainerHolder;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.HasStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.GraphStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.IdStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.NoOpBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.PropertiesStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.VertexStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 * @author Grant Haywood <a href="http://iowntheinter.net">http://iowntheinter.net</a>)
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyGraphStepStrategy extends FireflyStrategyBase {
    /**
     * Default constructor for FireflyGraphStepStrategy.
     */
    public FireflyGraphStepStrategy() {
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        if (ComputerHelper.onGraphComputer(traversal)) {
            return;
        }

        final boolean propertyRemovalValid = isPropertyRemovalValid(traversal);

        for (final GraphStep originalGraphStep : TraversalHelper.getStepsOfClass(GraphStep.class, traversal)) {
            int labelCount = 0;
            labelCount += originalGraphStep.getLabels().size();
            final FireflyGraphStep<?, ?> fireflyGraphStep = new FireflyGraphStep<>(originalGraphStep);
            TraversalHelper.replaceStep(originalGraphStep, fireflyGraphStep, traversal);
            Step<?, ?> currentStep = fireflyGraphStep.getNextStep();
            while (currentStep instanceof HasStep || currentStep instanceof NoOpBarrierStep) {
                if (currentStep instanceof HasStep) {
                    for (final HasContainer hasContainer : ((HasContainerHolder) currentStep).getHasContainers()) {
                        if (!GraphStep.processHasContainerIds(fireflyGraphStep, hasContainer))
                            fireflyGraphStep.addHasContainer(hasContainer);
                    }
                    TraversalHelper.copyLabels(currentStep, currentStep.getPreviousStep(), false);
                    traversal.removeStep(currentStep);
                }
                currentStep = currentStep.getNextStep();
            }
            if (propertyRemovalValid && labelCount == 0) {
                if (currentStep instanceof VertexStep || currentStep instanceof IdStep) {
                    final List<String> properties = new ArrayList<>();
                    fireflyGraphStep.getHasContainers().forEach(hasContainer -> properties.add(hasContainer.getKey()));
                    fireflyGraphStep.addProperties(properties);
                } else if (currentStep instanceof PropertiesStep) {
                    final PropertiesStep<?> propertiesStep = (PropertiesStep<?>) currentStep;
                    if (propertiesStep.getPropertyKeys().length != 0) {
                        final List<String> properties = new ArrayList<>();
                        fireflyGraphStep.getHasContainers().forEach(hasContainer -> properties.add(hasContainer.getKey()));
                        final String[] propertyKeys = propertiesStep.getPropertyKeys();
                        for (final String propertyKey : propertyKeys) {
                            if (!properties.contains(propertyKey)) {
                                properties.add(propertyKey);
                            }
                        }
                        fireflyGraphStep.addProperties(properties);
                    }
                }
            }
        }
    }
}
