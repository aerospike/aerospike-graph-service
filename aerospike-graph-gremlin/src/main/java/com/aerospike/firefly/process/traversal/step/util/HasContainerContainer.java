package com.aerospike.firefly.process.traversal.step.util;

import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.List;

public class HasContainerContainer implements Serializable {
    private final List<HasContainer> hasContainers;
    private List<HasContainer> fireflyHasContainers;
    private List<HasContainer> aerospikeHasContainers;
    private boolean initialized = false;

    public HasContainerContainer(final List<HasContainer> hasContainers) {
        this.hasContainers = hasContainers;
    }

    // prepare hasContainers if not already done.
    // can't do this in constructor because graphComputerView is empty there.
    public void init(final FireflyGraph graph) {
        if (initialized) {
            return;
        }

        final List<HasContainer> combinedHasContainers = HasContainerHelper.getVertexFilter(graph, hasContainers);

        // GRAPH-401: getFireflyHasContainers() intentionally returns the full list (every container in cardinality
        // order) rather than only the unsupported ones. Some callers (e.g. FireflyOtherVBatchReadStepLocal) apply
        // this list to elements that were never filtered server-side, so reducing it here would miss predicates.
        // Callers that have already pushed aerospikeHasContainers server-side should pick the reduced variant
        // themselves via FireflyBatchReadHelper.splitHasContainers(...).firefly.
        final FireflyBatchReadHelper.SplitHasContainers split = FireflyBatchReadHelper.splitHasContainers(
                graph, Vertex.class, combinedHasContainers);
        fireflyHasContainers = split.all;
        aerospikeHasContainers = split.aerospike;
        initialized = true;
    }

    public List<HasContainer> getFireflyHasContainers() {
        return fireflyHasContainers;
    }

    public List<HasContainer> getAerospikeHasContainers() {
        return aerospikeHasContainers;
    }
}
