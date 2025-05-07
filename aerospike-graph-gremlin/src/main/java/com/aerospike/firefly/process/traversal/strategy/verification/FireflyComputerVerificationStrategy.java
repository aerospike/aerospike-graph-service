package com.aerospike.firefly.process.traversal.strategy.verification;

import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ShortestPathVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.AbstractTraversalStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.VerificationException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public final class FireflyComputerVerificationStrategy extends AbstractTraversalStrategy<TraversalStrategy.VerificationStrategy> implements TraversalStrategy.VerificationStrategy {
    private static final FireflyComputerVerificationStrategy INSTANCE = new FireflyComputerVerificationStrategy();
    private static final Set<Class<?>> UNSUPPORTED_ALGORITHM_STEPS = new HashSet<>(Arrays.asList(
            ShortestPathVertexProgramStep.class));

    private FireflyComputerVerificationStrategy() {
    }

    @Override
    public void apply(final Traversal.Admin<?, ?> traversal) {
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
