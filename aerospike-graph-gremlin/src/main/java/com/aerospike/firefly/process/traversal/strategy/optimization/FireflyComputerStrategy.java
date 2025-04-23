package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.process.traversal.step.computer.VertexProgramProxyStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ComputerResultStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.ConnectedComponentVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.PageRankVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.TraversalVertexProgramStep;
import org.apache.tinkerpop.gremlin.process.computer.traversal.step.map.VertexProgramStep;
import org.apache.tinkerpop.gremlin.process.traversal.Step;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.util.TraversalHelper;

import java.util.List;

/*
    initial traversal: g.V(1,2,3).pageRank()
    after TinkerPop strategies:
        PageRankVertexProgramStep([VertexStep(OUT,edge)],gremlin.pageRankVertexProgram.pageRank,20,graphfilter[none])
        TraversalVertexProgramStep([GraphStep(vertex,[1, 2, 3])],graphfilter[none])
        ComputerResultStep
    target after this strategy:
        GraphStep - to start with all vertices
        PageRankVertexProgramStep
        ComputerResultStep
*/

public class FireflyComputerStrategy extends FireflyStrategyBase {
    private static final FireflyComputerStrategy INSTANCE = new FireflyComputerStrategy();

    private FireflyComputerStrategy() {
    }

    @Override
    public String getStrategyEnabledKey() {
        return null;
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        final List<Step> steps = traversal.getSteps();
        if (steps.size() < 3 || !isSupportedStep(steps.get(0))
                || !(steps.get(1) instanceof TraversalVertexProgramStep)
                || !(steps.get(2) instanceof ComputerResultStep)) {
            return;
        }
        final VertexProgramStep program = (VertexProgramStep) steps.get(0);
        final TraversalVertexProgramStep filter = (TraversalVertexProgramStep) steps.get(1);

        TraversalHelper.replaceStep(program, new VertexProgramProxyStep(program, filter), traversal);
        // remove TraversalVertexProgramStep
        traversal.removeStep(1);
    }

    private boolean isSupportedStep(final Step step) {
        return step instanceof PageRankVertexProgramStep || step instanceof ConnectedComponentVertexProgramStep;
    }

    public static FireflyComputerStrategy instance() {
        return INSTANCE;
    }
}
