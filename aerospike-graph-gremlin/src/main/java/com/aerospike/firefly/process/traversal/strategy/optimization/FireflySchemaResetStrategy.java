package com.aerospike.firefly.process.traversal.strategy.optimization;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Graph;

import java.util.Optional;

public class FireflySchemaResetStrategy extends FireflyStrategyBase {


    public FireflySchemaResetStrategy() {
    }

    @Override
    protected void doApply(final Traversal.Admin<?, ?> traversal) {
        if (!traversal.isRoot()) {
            return;
        }

        final Optional<Graph> graphOptional = traversal.getGraph();
        if (graphOptional.isEmpty()) {
            return;
        }
        if (!(graphOptional.get() instanceof FireflyGraph)) {
            return;
        }

        final FireflyGraph graph = (FireflyGraph) graphOptional.get();
        graph.getBaseGraph().schemaManager.resetThreadLocals();
    }
}
