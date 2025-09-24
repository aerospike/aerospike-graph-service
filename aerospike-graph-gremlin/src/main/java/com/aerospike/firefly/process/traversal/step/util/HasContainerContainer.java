package com.aerospike.firefly.process.traversal.step.util;

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.structure.FireflyGraph;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.io.Serializable;
import java.util.List;
import java.util.stream.Collectors;

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

        if (combinedHasContainers != null) {
            final List<FireflyGraphStep.HasContainerWithCardinality> hasContainerWithCardinalities =
                    FireflyBatchReadHelper.getHasContainersWithCardinalityOrder(graph, Vertex.class, combinedHasContainers);
            // TODO GRAPH-401: This is a hack to get around the fact that we cannot filter our cache with a hasContainer.
            //  To get around this we have to filter everything post read again, so all containers pushed to firefly no
            //  matter what.
            fireflyHasContainers = hasContainerWithCardinalities.stream().map(a -> a.hasContainer).collect(Collectors.toList());
            aerospikeHasContainers = FireflyBatchReadHelper.getAerospikeHasContainers(hasContainerWithCardinalities);
        } else {
            fireflyHasContainers = List.of();
            aerospikeHasContainers = List.of();
        }
        initialized = true;
    }

    public List<HasContainer> getFireflyHasContainers() {
        return fireflyHasContainers;
    }

    public List<HasContainer> getAerospikeHasContainers() {
        return aerospikeHasContainers;
    }
}
