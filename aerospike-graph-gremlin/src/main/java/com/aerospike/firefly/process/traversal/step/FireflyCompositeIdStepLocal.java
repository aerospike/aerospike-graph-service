package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.process.traversal.step.util.TraversalUtil;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.computer.util.ComputerGraph;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.LocalBarrier;
import org.apache.tinkerpop.gremlin.process.traversal.step.TraversalParent;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.AbstractStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.BulkSet;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.process.traversal.util.FastNoSuchElementException;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyCompositeIdStepLocal extends AbstractStep<Vertex, Vertex> implements LocalBarrier<Vertex> {
    private final Direction direction;
    private final Set<String> edgeLabels;

    // HasContainers to apply to the read of the composite id step to filter results.
    public final List<HasContainer> fireflyHasContainers;
    public final List<HasContainer> aerospikeHasContainers;
    private final int barrierSize;
    private final List<String> requiredProperties;
    private TraverserSet<Vertex> barrier;
    final Traversal.Admin traversal;
    final Set<String> labels;

    public FireflyCompositeIdStepLocal(final Traversal.Admin traversal,
                                       final Direction direction,
                                       final String[] edgeLabels,
                                       final Set<String> labels,
                                       final List<HasContainer> hasContainers,
                                       final int barrierSize,
                                       final List<String> requiredProperties) {
        super(traversal);
        this.traversal = traversal;
        this.direction = direction;
        this.barrier = (TraverserSet<Vertex>) traversal.getTraverserSetSupplier().get();
        this.edgeLabels = new HashSet<>(Arrays.asList(edgeLabels));
        this.labels = new HashSet<>(labels);
        this.barrierSize = barrierSize;
        if (hasContainers != null) {
            final List<FireflyGraphStep.HasContainerWithCardinality> hasContainerWithCardinalities =
                    FireflyBatchReadHelper.getHasContainersWithCardinalityOrder((FireflyGraph) traversal.getGraph().get(), Vertex.class, hasContainers);
            // TODO GRAPH-401: This is a hack to get around the fact that we cannot filter our cache with a hasContainer.
            //  To get around this we have to filter everything post read again, so all containers pushed to firefly no
            //  matter what.
            fireflyHasContainers = hasContainerWithCardinalities.stream().map(a -> a.hasContainer).collect(Collectors.toList());
            aerospikeHasContainers = FireflyBatchReadHelper.getAerospikeHasContainers(hasContainerWithCardinalities);
        } else {
            fireflyHasContainers = List.of();
            aerospikeHasContainers = List.of();
        }
        this.requiredProperties = requiredProperties;
    }

    @Override
    public void processAllStarts() {
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());

        // Create output traverser set since we cant append to the input while we are iterating.
        final TraverserSet<Vertex> output = new TraverserSet<>();

        // Info is used to keep track of how many output items we assign for each input (executed in order).
        final List<FireflyBatchReadHelper.ReadStepInfo<Vertex>> fireflyCompositeIdStepInfos = new ArrayList<>();
        final List<FireflyId> fireflyIdList = new ArrayList<>();
        final Set<FireflyId> uniqueIdSet = new HashSet<>();
        final Map<FireflyId, FireflyVertex> fireflyVertexMap = new TreeMap<>();

        final BulkSet<Object> set = new BulkSet<>();
        int count = 0;
        while (this.starts.hasNext()) {
            count++;
            // Get next input traverser and get the FireflyVertex form of it.
            final Traverser.Admin<Vertex> traverser = this.starts.next();
            traverser.setStepId(this.getNextStep().getId());
            final ComputerGraph.ComputerVertex computerVertex = (ComputerGraph.ComputerVertex) traverser.get();
            final FireflyVertex vertex = (FireflyVertex) computerVertex.getBaseVertex();

            // Latch the size of the current id list.
            final int previousSize = fireflyIdList.size();

            // All the work for supernode scan/index/cache handling is done in the getVertexIdsFromVertex function.
            TraversalUtil.supernodeTraversalWarning(graph, this.traversal, vertex);

            FireflyBatchReadHelper.addElementsToSet(
                    fireflyIdList, uniqueIdSet, fireflyVertexMap, vertex.getVertexIdsFromVertex(direction, edgeLabels));

            // Calculate how many ids were added by the function (size of list - previous size).
            // Create composite id info with this value and the appropriate traverser to the info list.
            fireflyCompositeIdStepInfos.add(new FireflyBatchReadHelper.ReadStepInfo<>(traverser, fireflyIdList.size() - previousSize));

            // If we reach or exceed batch size then execute so we don't use too much memory at any given point. Also drain if list size gets very big.
            if (uniqueIdSet.size() >= graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE ||
                    fireflyIdList.size() >= 5 * graph.getBaseGraph().AEROSPIKE_BATCH_READ_SIZE) {
                // Drain data to output.
                FireflyBatchReadHelper.drainDataToOutput(this, fireflyIdList, uniqueIdSet,
                        fireflyVertexMap, fireflyCompositeIdStepInfos, aerospikeHasContainers, fireflyHasContainers, output, graph::readVertices, requiredProperties, true);
            }
        }
        System.out.println("!!!!!!!! input -> " + count);

        // Drain data to output.
        FireflyBatchReadHelper.drainDataToOutput(this, fireflyIdList, uniqueIdSet,
                fireflyVertexMap, fireflyCompositeIdStepInfos, aerospikeHasContainers, fireflyHasContainers, output, graph::readVertices, requiredProperties, true);

        System.out.println("!!!!!!!! output -> " + output.size());
        this.barrier.addAll(output);
        output.clear(); // Force garbage collection.
    }

    @Override
    public boolean hasNextBarrier() {
        if (this.barrier.isEmpty()) {
            this.processAllStarts();
        }
        return !this.barrier.isEmpty();
    }

    @Override
    public TraverserSet<Vertex> nextBarrier() throws NoSuchElementException {
        if (this.barrier.isEmpty()) {
            this.processAllStarts();
        }
        if (this.barrier.isEmpty())
            throw FastNoSuchElementException.instance();
        else {
            final TraverserSet<Vertex> temp = this.barrier;
            this.barrier = (TraverserSet<Vertex>) this.traversal.getTraverserSetSupplier().get();
            return temp;
        }
    }

    @Override
    public void addBarrier(final TraverserSet<Vertex> barrier) {
        this.barrier.addAll(barrier);
    }

    @Override
    public void reset() {
        System.out.println("!!!!!!!! Reset -> " + this.barrier.size());
        super.reset();
        this.barrier.clear();
    }

    @Override
    protected Traverser.Admin<Vertex> processNextStart() throws NoSuchElementException {
        System.out.println("!!!!!!!! processNextStart -> " + this.barrier.size());
        if (this.barrier.isEmpty()) {
            this.processAllStarts();
        }
        return this.barrier.remove();
    }
}
