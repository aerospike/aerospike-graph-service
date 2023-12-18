package com.aerospike.firefly.process.traversal.step;

import com.aerospike.firefly.process.traversal.step.sideEffect.FireflyGraphStep;
import com.aerospike.firefly.process.traversal.step.util.FireflyBatchReadHelper;
import com.aerospike.firefly.structure.FireflyGraph;
import com.aerospike.firefly.structure.FireflyVertex;
import com.aerospike.firefly.structure.id.FireflyId;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.Traverser;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.CollectingBarrierStep;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.apache.tinkerpop.gremlin.process.traversal.traverser.util.TraverserSet;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * @author Lyndon Bauto (<a href="https://github.com/lyndonbauto">https://github.com/lyndonbauto</a>)
 */
public class FireflyCompositeIdStep extends CollectingBarrierStep<Vertex> {
    private final Direction direction;
    private final Set<String> edgeLabels;

    // HasContainers to apply to the read of the composite id step to filter results.
    public final List<HasContainer> fireflyHasContainers;
    public final List<HasContainer> aerospikeHasContainers;
    private final long sampleSize;
    private final int barrierSize;

    public FireflyCompositeIdStep(final Traversal.Admin traversal,
                                  final Direction direction,
                                  final String[] edgeLabels,
                                  final Set<String> labels,
                                  final List<HasContainer> hasContainers,
                                  final long sampleSize,
                                  final int barrierSize) {
        super(traversal, barrierSize);
        this.direction = direction;
        this.edgeLabels = new HashSet<>(Arrays.asList(edgeLabels));
        this.labels = new HashSet<>(labels);
        this.sampleSize = sampleSize;
        this.barrierSize = barrierSize;
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
    }

    @Override
    public void barrierConsumer(final TraverserSet<Vertex> set) {
        final FireflyGraph graph = ((FireflyGraph) getTraversal().getGraph().get());
        FireflyBatchReadHelper.pullFromLeft(traversal, graph, set, barrierSize);

        // Create output traverser set since we cant append to the input while we are iterating.
        final TraverserSet<Vertex> output = new TraverserSet<>();

        // Info is used to keep track of how many output items we assign for each input (executed in order).
        final List<FireflyBatchReadHelper.ReadStepInfo<Vertex>> fireflyCompositeIdStepInfos = new ArrayList<>();
        final List<FireflyId> fireflyIdList = new ArrayList<>();
        final Set<FireflyId> uniqueIdSet = new HashSet<>();
        final Map<FireflyId, FireflyVertex> fireflyVertexMap = new TreeMap<>();

        if (sampleSize != -1) {
            // For a sample we need to yank all the ids from the input vertices and then randomly select ones from that.
            final Map<Traverser.Admin<Vertex>, FireflyVertex> inputVertices = new HashMap<>();
            while (!set.isEmpty()) {
                final Traverser.Admin<Vertex> traverser = set.remove();
                final FireflyVertex vertex = (FireflyVertex) traverser.get();
                inputVertices.put(traverser, vertex);
            }

            // Create mapping of input vertex to output vertex ids.
            final Map<Traverser.Admin<Vertex>, List<FireflyId>> outputVertexIds = new HashMap<>();
            for (final Traverser.Admin<Vertex> input : inputVertices.keySet()) {
                final FireflyVertex vertex = inputVertices.get(input);
                outputVertexIds.put(input, vertex.getVertexIdsFromVertex(direction, edgeLabels));
            }

            // Create ordered list of input traversers and output vertex ids.
            final List<Traverser.Admin<Vertex>> orderedInputTraversers = new ArrayList<>(inputVertices.keySet());
            final List<List<FireflyId>> orderedOutputVertexIds = new ArrayList<>();
            for (final Traverser.Admin<Vertex> orderedInputTraverser : orderedInputTraversers) {
                orderedOutputVertexIds.add(outputVertexIds.get(orderedInputTraverser));
            }

            // Count the total number of output vertex ids.
            final AtomicLong count = new AtomicLong(0);
            orderedOutputVertexIds.forEach(vertexIds -> count.addAndGet(vertexIds.size()));

            // Generate random indices. This method is used so that we don't spin an RNG generator a ridiculous amount
            // of times in the event that we have something like 1,000,000 elements and a sample size of 999,999.
            final Set<Long> randomIndices = new HashSet<>();
            final List<Long> values = LongStream.range(0, count.get()).boxed().collect(Collectors.toList());
            Collections.shuffle(values);
            final long indexCount = Math.min(this.sampleSize, count.get());
            for (long i = 0; i < indexCount; i++) {
                randomIndices.add(values.get((int) i));
            }

            // Allow garbage collection since this may take up a relatively large amount of memory
            values.clear();

            // Create list of sampled vertex ids.
            final List<FireflyId> sampledVertexIds = new ArrayList<>();
            int currentIndex = 0;
            for (int i = 0; i < orderedOutputVertexIds.size(); i++) {
                final List<FireflyId> vertexIds = orderedOutputVertexIds.get(i);
                for (int j = 0; j < vertexIds.size(); j++) {
                    if (randomIndices.contains((long) currentIndex + j)) {
                        sampledVertexIds.add(vertexIds.get(j));
                    }
                }
                currentIndex += vertexIds.size();
            }

            // Read the sampled vertices.
            final Map<FireflyId, FireflyVertex> vertexMap = new HashMap<>();
            FireflyBatchReadHelper.populateElementMap(
                    new HashSet<>(sampledVertexIds), vertexMap, aerospikeHasContainers, graph::readVertices);

            // Create list of random indices to sample and order them in ascending order so we can iterate through them.
            final List<Long> randomIndicesList = new ArrayList<>(randomIndices);
            randomIndicesList.sort(Comparator.naturalOrder());

            // Loop variables.
            int indexId = 0;
            long index = 0;
            boolean exit = false;

            // Loop through the input traversers and output vertex ids and split the input traverser into the output.
            for (int i = 0; i < orderedInputTraversers.size(); i++) {
                for (int j = 0; j < orderedOutputVertexIds.get(i).size(); j++) {
                    if (randomIndicesList.get(indexId) == index) {
                        indexId++;
                        output.add(orderedInputTraversers.get(i).split(vertexMap.get(orderedOutputVertexIds.get(i).get(j)), this));
                        if (indexId == randomIndicesList.size()) {
                            // We have reached the end of the random indices so we can exit the loops.
                            exit = true;
                            break;
                        }
                    }
                    index++;
                }
                if (exit) {
                    break;
                }
            }
        } else {
            while (!set.isEmpty()) {
                // Get next input traverser and get the FireflyVertex form of it.
                final Traverser.Admin<Vertex> traverser = set.remove();
                final FireflyVertex vertex = (FireflyVertex) traverser.get();

                // Latch the size of the current id list.
                final int previousSize = fireflyIdList.size();

                // All the work for supernode scan/index/cache handling is done in the getVertexIdsFromVertex function.
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
                            fireflyVertexMap, fireflyCompositeIdStepInfos, aerospikeHasContainers, fireflyHasContainers, output, graph::readVertices);
                }
            }

            // Drain data to output.
            FireflyBatchReadHelper.drainDataToOutput(this, fireflyIdList, uniqueIdSet,
                    fireflyVertexMap, fireflyCompositeIdStepInfos, aerospikeHasContainers, fireflyHasContainers, output, graph::readVertices);
        }

        set.addAll(output);
        output.clear(); // Force garbage collection.
    }
}
