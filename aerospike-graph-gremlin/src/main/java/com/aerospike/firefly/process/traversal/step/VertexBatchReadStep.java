package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.TraverserRequirement;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.EmptyTraverser;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class VertexBatchReadStep extends CollectingBarrierStep<Edge> implements LocalBarrier<Edge> {
    protected final List<HasContainer> fireflyHasContainers;
    protected final List<HasContainer> aerospikeHasContainers;
    protected final int barrierSize;
    protected final boolean areEdgesRequired;
    private final long limit;
    private long runningTotal = 0;

    public VertexBatchReadStep(final Traversal.Admin traversal,
                               final List<HasContainer> hasContainers,
                               final Set<String> labels,
                               final int barrierSize,
                               final boolean areEdgesRequired,
                               final long limit) {
        super(traversal, barrierSize);
        this.areEdgesRequired = areEdgesRequired;

        this.labels = new HashSet<>(labels);
        this.barrierSize = barrierSize;

        // piece of magic
        if (hasContainers != null) {
            final List<FireflyGraphStep.HasContainerWithCardinality> hasContainerWithCardinalities =
                    FireflyBatchReadHelper.getHasContainersWithCardinalityOrder((FireflyGraph) getTraversal().getGraph().get(), Vertex.class, hasContainers);
            // TODO GRAPH-401: This is a hack to get around the fact that we cannot filter our cache with a hasContainer.
            //  To get around this we have to filter everything post read again, so all containers pushed to firefly no
            //  matter what.
            fireflyHasContainers = hasContainerWithCardinalities.stream().map(a -> a.hasContainer).collect(Collectors.toList());
            aerospikeHasContainers = FireflyBatchReadHelper.getAerospikeHasContainers(hasContainerWithCardinalities);
        } else {
            fireflyHasContainers = List.of();
            aerospikeHasContainers = List.of();
        }
        this.limit = limit;
    }

    @Override
    public void barrierConsumer(final TraverserSet<Edge> set) {
        if (limit != -1 && runningTotal >= limit) {
            set.clear();
            return; // Limit reached in previous barrier consumer, stop processing.
        }
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());
        FireflyBatchReadHelper.pullFromLeft(traversal, graph, set, barrierSize);

        // Create output traverser set since we cant append to the input while we are iterating.
        final TraverserSet<Edge> output = new TraverserSet<>();

        // Info is used to keep track of how many output items we assign for each input (executed in order).
        final List<FireflyBatchReadHelper.ReadStepInfo<Edge>> fireflyBatchEdgeReadStepInfos = new ArrayList<>();
        final List<FireflyId> fireflyIdList = new ArrayList<>();
        final Set<FireflyId> uniqueIdSet = new HashSet<>();
        final Map<FireflyId, FireflyVertex> fireflyVertexMap = new HashMap<>();

        while (!set.isEmpty()) {
            final Traverser.Admin traverser = set.remove();

            final List<FireflyId> ids = getVertexIds(traverser);
            if (ids.isEmpty()) continue;

            final int previousSize = fireflyIdList.size();
            for (final FireflyId id : ids) {
                fireflyIdList.add(id);
                if (!fireflyVertexMap.containsKey(id)) {
                    uniqueIdSet.add(id);
                }
            }

            fireflyBatchEdgeReadStepInfos.add(new FireflyBatchReadHelper.ReadStepInfo<>(traverser, fireflyIdList.size() - previousSize));

            if (uniqueIdSet.size() >= graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE ||
                    fireflyIdList.size() >= 5 * graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE) {
                runningTotal += FireflyBatchReadHelper.drainDataToOutput(this, fireflyIdList, uniqueIdSet,
                        fireflyVertexMap, fireflyBatchEdgeReadStepInfos, aerospikeHasContainers, fireflyHasContainers, output, graph::readVertices, null, areEdgesRequired);
                if (limit != -1 && runningTotal >= limit) {
                    if (output.isEmpty()) {
                        set.add(EmptyTraverser.instance());
                    } else {
                        set.clear();
                        set.addAll(output);
                        output.clear(); // Force garbage collection.
                    }
                    return; // Limit reached, stop processing.
                }
            }
        }

        runningTotal += FireflyBatchReadHelper.drainDataToOutput(this, fireflyIdList, uniqueIdSet,
                fireflyVertexMap, fireflyBatchEdgeReadStepInfos, aerospikeHasContainers, fireflyHasContainers, output, graph::readVertices, null, areEdgesRequired);

        if (output.isEmpty()) {
            set.add(EmptyTraverser.instance());
        } else {
            set.clear();
            set.addAll(output);
            output.clear(); // Force garbage collection.
        }
    }

    protected abstract List<FireflyId> getVertexIds(final Traverser.Admin traverser);

    @Override
    public Set<TraverserRequirement> getRequirements() {
        return Collections.singleton(TraverserRequirement.OBJECT);
    }
}
